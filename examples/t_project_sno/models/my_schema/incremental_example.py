# Metadata for incremental_example.sql
from tee.typing.metadata import ModelMetadataDict

# Example 1: Append-only incremental
metadata_append: ModelMetadataDict = {
    "description": "Incremental table using append strategy",
    "materialization": "incremental",
    "incremental": {
        "strategy": "append",
        "append": {
            "time_column": "created_at",
            "start_date": "@start_date",  # Use variable from CLI
            "lookback": "7 days"
        }
    }
}

# Example 2: Merge incremental
metadata_merge: ModelMetadataDict = {
    "description": "Incremental table using merge strategy",
    "materialization": "incremental", 
    "incremental": {
        "strategy": "merge",
        "merge": {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",  # Will use max(time_column) from target table
            "lookback": "3 hours"
        }
    }
}

# Example 3: Delete+insert incremental
metadata_delete_insert: ModelMetadataDict = {
    "description": "Incremental table using delete+insert strategy",
    "materialization": "incremental",
    "incremental": {
        "strategy": "delete_insert",
        "delete_insert": {
            "where_condition": "updated_at >= '@start_date'",
            "time_column": "updated_at",
            "start_date": "@start_date",
        }
    }
}

# Choose which strategy to use by assigning to 'metadata'
# Simply change the assignment below to test different strategies:
# metadata = metadata_append  # Uncomment to use append strategy
# metadata = metadata_merge  # Uncomment to use merge strategy
metadata = metadata_delete_insert  # Currently using delete+insert strategy
