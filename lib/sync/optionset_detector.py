"""Detect option sets from API response data."""

from dataclasses import dataclass


@dataclass
class DetectedOptionSet:
    """An option set detected from API response."""

    field_name: str
    is_multi_select: bool
    codes_and_labels: dict[int, str]  # {1: "Active", 2: "Inactive"}


class OptionSetDetector:
    """Detects option sets from API response records."""

    def detect_from_record(self, api_record: dict) -> dict[str, DetectedOptionSet]:
        """
        Detect option sets from a single API record.

        Args:
            api_record: API response record

        Returns:
            Dict mapping field name to DetectedOptionSet
        """
        detected = {}

        for key in api_record:
            # Look for @FormattedValue annotations
            if key.endswith("@OData.Community.Display.V1.FormattedValue"):
                # Extract base field name
                field_name = key.replace(
                    "@OData.Community.Display.V1.FormattedValue", ""
                )

                # Get raw value
                raw_value = api_record.get(field_name)
                formatted_value = api_record.get(key)

                if raw_value is None or formatted_value is None:
                    continue

                # Determine if multi-select
                is_multi_select = self._is_multi_select(raw_value, formatted_value)

                # Extract codes and labels
                codes_and_labels = self._extract_codes_and_labels(
                    raw_value, formatted_value, is_multi_select
                )

                if codes_and_labels:
                    detected[field_name] = DetectedOptionSet(
                        field_name=field_name,
                        is_multi_select=is_multi_select,
                        codes_and_labels=codes_and_labels,
                    )

        return detected

    def _is_multi_select(self, raw_value: any, formatted_value: str) -> bool:
        """
        Determine if this is a multi-select option set.

        Multi-select indicators:
        - Formatted value contains semicolons
        - Raw value is comma-separated string of codes
        """
        if isinstance(formatted_value, str) and ";" in formatted_value:
            return True
        if isinstance(raw_value, str) and "," in raw_value:
            return True
        return False

    def _extract_codes_and_labels(
        self, raw_value: any, formatted_value: str, is_multi_select: bool
    ) -> dict[int, str]:
        """
        Extract code-label mappings.

        Args:
            raw_value: Raw integer code(s) from API
            formatted_value: Formatted label(s) from API
            is_multi_select: Whether this is multi-select

        Returns:
            Dict mapping code to label
        """
        codes_and_labels = {}

        try:
            if is_multi_select:
                # Multi-select: Parse comma-separated codes and semicolon-separated labels
                if isinstance(raw_value, str):
                    codes = [
                        int(c.strip()) for c in raw_value.split(",") if c.strip()
                    ]
                else:
                    # Sometimes multi-select raw values are already integers
                    codes = [int(raw_value)]

                labels = [
                    label.strip()
                    for label in formatted_value.split(";")
                    if label.strip()
                ]

                # Match codes to labels
                for code, label in zip(codes, labels):
                    codes_and_labels[code] = label

            else:
                # Single-select: Direct mapping
                code = int(raw_value)
                codes_and_labels[code] = formatted_value

        except (ValueError, TypeError):
            # Skip if we can't parse as integer
            pass

        return codes_and_labels
