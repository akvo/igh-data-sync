"""Tests for option set detector."""

from igh_data_sync.sync.optionset_detector import OptionSetDetector


class TestOptionSetDetector:
    """Test option set detection from API records."""

    def test_detect_single_select(self):
        """Test detecting single-select option set."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
            "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
        }

        detected = detector.detect_from_record(api_record)

        assert "statuscode" in detected
        option_set = detected["statuscode"]
        assert option_set.field_name == "statuscode"
        assert option_set.is_multi_select is False
        assert option_set.codes_and_labels == {1: "Active"}

    def test_detect_multi_select(self):
        """Test detecting multi-select option set."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "categories": "1,2",
            "categories@OData.Community.Display.V1.FormattedValue": "Category A;Category B",
        }

        detected = detector.detect_from_record(api_record)

        assert "categories" in detected
        option_set = detected["categories"]
        assert option_set.field_name == "categories"
        assert option_set.is_multi_select is True
        assert option_set.codes_and_labels == {1: "Category A", 2: "Category B"}

    def test_detect_multiple_option_sets(self):
        """Test detecting multiple option sets in one record."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
            "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
            "categories": "1,2",
            "categories@OData.Community.Display.V1.FormattedValue": "Category A;Category B",
        }

        detected = detector.detect_from_record(api_record)

        assert len(detected) == 2
        assert "statuscode" in detected
        assert "categories" in detected

    def test_ignore_non_integer_codes(self):
        """Test ignoring fields with non-integer codes."""
        detector = OptionSetDetector()

        api_record = {
            "name": "Test Account",
            "name@OData.Community.Display.V1.FormattedValue": "Test Account",
        }

        detected = detector.detect_from_record(api_record)

        # Should not detect 'name' as option set (not an integer)
        assert "name" not in detected

    def test_missing_formatted_value(self):
        """Test handling missing formatted value."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
            # No @FormattedValue annotation
        }

        detected = detector.detect_from_record(api_record)

        # Should not detect statuscode without formatted value
        assert "statuscode" not in detected
