"""Tests for reference verifier with SCD2 temporal tables."""

import pytest

from lib.sync.database import DatabaseManager
from lib.sync.reference_verifier import ReferenceVerifier
from lib.sync.relationship_graph import EntityRelationships, RelationshipGraph


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test_verify.db"
    return str(db_path)


@pytest.fixture
def db_manager(temp_db):
    """Create database manager with SCD2 tables."""
    db = DatabaseManager(temp_db)
    db.connect()

    # Create vin_diseases table (referenced table) with SCD2 structure
    db.execute("""
        CREATE TABLE vin_diseases (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_diseaseid TEXT NOT NULL,
            vin_name TEXT,
            valid_from TEXT,
            valid_to TEXT
        )
    """)

    # Create vin_candidates table (referencing table) with SCD2 structure
    db.execute("""
        CREATE TABLE vin_candidates (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_candidateid TEXT NOT NULL,
            vin_name TEXT,
            _vin_disease_value TEXT,
            valid_from TEXT,
            valid_to TEXT
        )
    """)

    yield db
    db.close()


@pytest.fixture
def relationship_graph():
    """Create a relationship graph with vin_candidates → vin_diseases FK."""
    graph = RelationshipGraph()
    graph.relationships["vin_candidates"] = EntityRelationships(
        references_to=[
            # 3-tuple: (table, fk_column, referenced_column)
            ("vin_diseases", "_vin_disease_value", "vin_diseaseid")
        ],
        referenced_by=[],
    )
    graph.relationships["vin_diseases"] = EntityRelationships(
        references_to=[],
        referenced_by=[("vin_candidates", "_vin_disease_value", "vin_diseaseid")],
    )
    return graph


class TestReferenceVerifierSCD2:
    """Test reference verification with SCD2 temporal tables."""

    def test_verify_valid_fk_references_business_key(self, db_manager, relationship_graph):
        """Test valid FK references to business key (not surrogate key)."""
        # Insert disease with 2 versions (same business key, different row_ids)
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS", "2020-01-01", "2021-01-01"),
        )
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS v2", "2021-01-01", None),
        )

        # Insert candidate that references disease by business key (not row_id)
        db_manager.execute(
            "INSERT INTO vin_candidates "
            "(vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-1", "Candidate 1", "guid-hiv-123", "2021-06-01", None),
        )

        # Verify references
        verifier = ReferenceVerifier()
        report = verifier.verify_references(db_manager, relationship_graph)

        # Should pass - FK references business key which exists across all versions
        assert report.total_checks == 1
        assert len(report.issues) == 0
        assert report.total_issues == 0

    def test_verify_detects_dangling_fk_with_business_key(self, db_manager, relationship_graph):
        """Test that dangling FK is detected when business key doesn't exist."""
        # Insert disease
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS", "2020-01-01", None),
        )

        # Insert candidate with non-existent disease reference
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-1", "Candidate 1", "guid-malaria-999", "2021-06-01", None),
        )

        # Verify references
        verifier = ReferenceVerifier()
        report = verifier.verify_references(db_manager, relationship_graph)

        # Should fail - FK references non-existent business key
        assert report.total_checks == 1
        assert len(report.issues) == 1
        assert report.total_issues == 1
        assert report.issues[0].table == "vin_candidates"
        assert report.issues[0].fk_column == "_vin_disease_value"
        assert report.issues[0].referenced_table == "vin_diseases"
        assert report.issues[0].dangling_count == 1
        assert "guid-malaria-999" in report.issues[0].sample_ids

    def test_verify_with_multiple_versions_same_business_key(self, db_manager, relationship_graph):
        """Test that FK verification works correctly with multiple versions of referenced entity."""
        # Insert disease with 3 versions (same business key)
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV", "2020-01-01", "2021-01-01"),
        )
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS", "2021-01-01", "2022-01-01"),
        )
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS v3", "2022-01-01", None),
        )

        # Insert multiple candidates referencing the same disease
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-1", "Candidate 1", "guid-hiv-123", "2021-06-01", None),
        )
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-2", "Candidate 2", "guid-hiv-123", "2021-07-01", None),
        )

        # Verify references
        verifier = ReferenceVerifier()
        report = verifier.verify_references(db_manager, relationship_graph)

        # Should pass - both FKs reference valid business key (exists in all 3 versions)
        assert report.total_checks == 1
        assert len(report.issues) == 0
        assert report.total_issues == 0

    def test_verify_mixed_valid_and_invalid_fks(self, db_manager, relationship_graph):
        """Test verification with mix of valid and invalid FK references."""
        # Insert 2 diseases
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS", "2020-01-01", None),
        )
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-tb-456", "Tuberculosis", "2020-01-01", None),
        )

        # Insert candidates: 2 valid, 1 invalid
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-1", "Candidate 1", "guid-hiv-123", "2021-06-01", None),
        )
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-2", "Candidate 2", "guid-tb-456", "2021-07-01", None),
        )
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-3", "Candidate 3", "guid-malaria-999", "2021-08-01", None),
        )

        # Verify references
        verifier = ReferenceVerifier()
        report = verifier.verify_references(db_manager, relationship_graph)

        # Should detect 1 dangling FK
        assert report.total_checks == 1
        assert len(report.issues) == 1
        assert report.total_issues == 1
        assert report.issues[0].dangling_count == 1
        assert report.issues[0].total_checked == 3  # 3 total FK values
        assert "guid-malaria-999" in report.issues[0].sample_ids

    def test_verify_null_fk_values_ignored(self, db_manager, relationship_graph):
        """Test that NULL FK values are ignored (not reported as dangling)."""
        # Insert disease
        db_manager.execute(
            "INSERT INTO vin_diseases (vin_diseaseid, vin_name, valid_from, valid_to) VALUES (?, ?, ?, ?)",
            ("guid-hiv-123", "HIV/AIDS", "2020-01-01", None),
        )

        # Insert candidates with NULL FK (optional relationship)
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-1", "Candidate 1", None, "2021-06-01", None),
        )
        db_manager.execute(
            "INSERT INTO vin_candidates (vin_candidateid, vin_name, _vin_disease_value, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?, ?)",
            ("guid-candidate-2", "Candidate 2", "guid-hiv-123", "2021-07-01", None),
        )

        # Verify references
        verifier = ReferenceVerifier()
        report = verifier.verify_references(db_manager, relationship_graph)

        # Should pass - NULL FK is ignored
        assert report.total_checks == 1
        assert len(report.issues) == 0
        assert report.total_issues == 0
