from sqlglot import exp
from tee.parser.model_decorator import model


@model(table_name="users_summary", description="Summary of user data")
def create_users_summary():
    """Create a summary table of users with aggregated data."""
    return exp.select("*").from_("my_first_table")


@model(table_name="recent_users")
def get_recent_users():
    """Get users created recently."""
    return exp.select("*").from_("my_first_table")


@model(table_name="complex_join")
def create_complex_join():
    """Create a complex join between multiple tables."""
    return exp.select("*").from_("my_first_table")
