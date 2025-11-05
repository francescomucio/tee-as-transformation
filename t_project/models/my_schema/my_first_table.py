# Metadata for my_first_table.sql
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "schema": [
        {
            "name": "id",
            "datatype": "number",
            "description": "Unique identifier for the record",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "name",
            "datatype": "string",
            "description": "Name of the record",
            "tests": ["not_null"]
        }
    ],
    "partitions": ["id"],
    "materialization": "table",
    "tests": [
        "row_count_gt_0"
        # Example: "example_custom_test" - Custom SQL test (requires table to have 5+ rows)
        # Example: {"name": "check_minimum_rows", "params": {"min_rows": 3}}
    ]
}
