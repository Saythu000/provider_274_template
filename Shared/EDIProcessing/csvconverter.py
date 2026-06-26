import os
import json
import csv
from typing import Dict, List, Any, Union


class CSVConverter:
    """
    Production-grade CSV converter that transforms mapped EDI data
    to schema-compliant CSV files. Designed to work with any schema
    file structure without code changes.
    Handles both single records and multiple records (lists).
    """

    # Fields that should preserve leading zeros (not true integers)
    CODE_FIELDS = ['RACE', 'RACECODE', 'ETHNICITY', 'LANGUAGECODE']

    def __init__(self, schemas_dir: str):
        """
        Initialize converter with schema directory path.

        Args:
            schemas_dir: Absolute path to directory containing schema JSON files
        """
        self.schemas_dir = schemas_dir
        self._validate_schema_dir()

    def _validate_schema_dir(self):
        """Ensure schema directory exists and is accessible."""
        if not os.path.exists(self.schemas_dir):
            raise FileNotFoundError(f"Schema directory not found: {self.schemas_dir}")
        if not os.path.isdir(self.schemas_dir):
            raise NotADirectoryError(f"Path is not a directory: {self.schemas_dir}")

    def _load_schema(self, schema_filename: str) -> List[Dict[str, Any]]:
        """
        Load and parse schema file, returning sorted field definitions.

        Args:
            schema_filename: Name of schema file (e.g., 'provider_hierarchy_7.12_schema.json')

        Returns:
            List of field definitions sorted by OrdinalPosition
        """
        schema_path = os.path.join(self.schemas_dir, schema_filename)

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema_data = json.load(f)

        fields = schema_data.get("columnNames", [])
        if not fields:
            raise ValueError(f"Schema file contains no column definitions: {schema_filename}")

        return sorted(fields, key=lambda x: x.get("OrdinalPosition", 0))

    def _is_date_field(self, field_name: str) -> bool:
        """Detect if field represents a date based on naming convention."""
        name_lower = field_name.lower()
        return any([
            "date" in name_lower,
            "dob" in name_lower,
            name_lower.endswith("dt"),
            name_lower.endswith("dtline"),
            "servicefrom" in name_lower,
            "servicethru" in name_lower
        ])

    def _format_date(self, val_str: str) -> str:
        """
        Convert date string to MM/dd/yyyy format.
        Handles YYYYMMDD and YYYY-MM-DD formats.
        """
        if len(val_str) == 8 and val_str.isdigit():
            return f"{val_str[4:6]}/{val_str[6:8]}/{val_str[0:4]}"
        elif len(val_str) == 10 and "-" in val_str:
            parts = val_str.split("-")
            if len(parts) == 3 and len(parts[0]) == 4:
                return f"{parts[1]}/{parts[2]}/{parts[0]}"
        return val_str

    def _format_value(self, value: Any, field_name: str, data_type: str) -> str:
        """
        Format value according to field type and schema data type.

        Args:
            value:      Raw value from mapped data
            field_name: Name of the field
            data_type:  Data type from schema (string, integer, decimal)

        Returns:
            Formatted string value (empty string for missing values)
        """
        if value is None or value == "":
            return ""

        val_str = str(value).strip()
        if not val_str:
            return ""

        # Format dates
        if self._is_date_field(field_name):
            return self._format_date(val_str)

        # Code fields preserve leading zeros (not true integers)
        if field_name in self.CODE_FIELDS:
            return val_str

        # Format integers
        if data_type == "integer":
            try:
                return str(int(float(val_str)))
            except ValueError:
                return ""

        # Format decimals
        if data_type == "decimal":
            try:
                return f"{float(val_str):.2f}"
            except ValueError:
                return ""

        return val_str

    def _ensure_output_directory(self, output_path: str):
        """Create output directory if it doesn't exist."""
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

    def _build_row(
        self,
        mapped_data: Dict[str, Any],
        fields: List[Dict[str, Any]],
        template_value: str = "TEMPLATE"
    ) -> List[str]:
        """
        Build a single CSV row from mapped data and field definitions.
        """
        row = []
        for field in fields:
            field_name = field["FieldName"]
            data_type  = field.get("DataType", "string")

            if field_name == "TEMPLATE":
                val = template_value
            else:
                val = mapped_data.get(field_name, "")

            formatted_val = self._format_value(val, field_name, data_type)
            row.append(formatted_val)

        return row

    def convert_to_csv(
        self,
        mapped_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        schema_filename: str,
        output_csv_path: str,
        template_value: str = "TEMPLATE"
    ) -> str:
        """
        Generic method to convert mapped data to CSV based on any schema.
        Handles both single record (dict) and multiple records (list of dicts).

        Args:
            mapped_data:      Dictionary or list of dictionaries of mapped field values
            schema_filename:  Name of schema file to use
            output_csv_path:  Path where CSV file will be written
            template_value:   Value for TEMPLATE field if present in schema

        Returns:
            Path to generated CSV file
        """
        fields  = self._load_schema(schema_filename)
        headers = [f["FieldName"] for f in fields]

        self._ensure_output_directory(output_csv_path)

        # Normalize input to list format
        data_list = mapped_data if isinstance(mapped_data, list) else [mapped_data]

        with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)
            for record in data_list:
                row = self._build_row(record, fields, template_value)
                writer.writerow(row)

        return output_csv_path

    def convert_providers(
        self,
        mapped_providers: Union[Dict[str, Any], List[Dict[str, Any]]],
        output_csv_path: str,
        schema_filename: str = "provider_7.12_schema.json"
    ) -> str:
        """Convert provider mapped data to schema-compliant CSV."""
        return self.convert_to_csv(mapped_providers, schema_filename, output_csv_path)

    def convert_hierarchy(
        self,
        mapped_hierarchy: Union[Dict[str, Any], List[Dict[str, Any]]],
        output_csv_path: str,
        schema_filename: str = "provider_hierarchy_7.12_schema.json"
    ) -> str:
        """Convert provider hierarchy mapped data to schema-compliant CSV."""
        return self.convert_to_csv(mapped_hierarchy, schema_filename, output_csv_path)
