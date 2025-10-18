# Example of typed metadata with full IDE support
from tee.typing.metadata import ModelMetadataDict

# This provides full type checking, autocomplete, and IDE support
metadata: ModelMetadataDict = {
    "schema": [
        {
            "name": "user_id",
            "datatype": "number",
            "description": "Unique user identifier",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "username",
            "datatype": "string",
            "description": "User's chosen username",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "email",
            "datatype": "string",
            "description": "User's email address",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "created_at",
            "datatype": "timestamp",
            "description": "Account creation timestamp",
            "tests": ["not_null"]
        },
        {
            "name": "is_active",
            "datatype": "boolean",
            "description": "Whether the user account is active",
            "tests": ["not_null"]
        },
        {
            "name": "profile_data",
            "datatype": "json",
            "description": "Additional user profile information",
            "tests": []
        }
    ],
    "partitions": ["created_at"],
    "materialization": "table",
    "tests": ["row_count_gt_0", "no_duplicates"]
}
