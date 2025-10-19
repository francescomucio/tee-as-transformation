# Metadata for my_second_table.sql
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "schema": [
        {
            "name": "id",
            "datatype": "number",
            "description": "Foreign key reference to my_first_table"
        },
        {
            "name": "name",
            "datatype": "string",
            "description": "Additional information",
            "tests": ["not_null"]
        }
    ],
    "materialization": "view",
    "tests": ["row_count_gt_0", "no_duplicates"]
}
