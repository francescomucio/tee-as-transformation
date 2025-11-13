"""
Test using @test decorator pattern.

This test checks that the 'id' column doesn't contain NULL values.
"""

from tee.testing import test


@test(name="check_no_null_ids", severity="error", description="Check that id column has no NULL values")
def check_no_null_ids():
    """Test SQL defined in Python function."""
    return """
    SELECT id
    FROM @table_name
    WHERE id IS NULL
    """

