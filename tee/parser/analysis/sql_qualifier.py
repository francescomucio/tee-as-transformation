"""
SQL qualification functionality for resolving table references.
"""

import re
import logging
from typing import List

from ..shared.exceptions import ParserError

# Configure logging
logger = logging.getLogger(__name__)


def generate_qualified_sql(sql_str: str, tables: List[str], table_name: str) -> str:
    """
    Generate qualified SQL by resolving table references.
    
    Args:
        sql_str: Original SQL string
        tables: List of table references found in the SQL
        table_name: Full table name with schema (e.g., my_schema.users_summary)
        
    Returns:
        Qualified SQL string with schema prefixes
    """
    try:
        # Get the schema from the table name if it exists
        if '.' in table_name:
            schema_name = table_name.split('.')[0]
        else:
            schema_name = None
        
        if not schema_name:
            return sql_str
        
        # Debug logging
        logger.debug(f"SQL qualifier: table_name={table_name}, schema_name={schema_name}, tables={tables}")
        
        # Find all table references and qualify them if they're not already qualified
        for table_ref in tables:
            logger.debug(f"Processing table reference: {table_ref}")
            # Skip if table is already fully qualified (contains a dot)
            # This includes both schema.table and database.schema.table formats
            if '.' in table_ref:
                logger.debug(f"Skipping qualified table reference: {table_ref}")
                continue
                
            if not table_ref.startswith(schema_name + '.'):
                # Check if the table reference is already qualified in the SQL
                # Look for patterns like "FROM table_name" or "JOIN table_name" where table_name is not already qualified
                # Use a more sophisticated pattern that checks for table references in FROM/JOIN clauses
                pattern = r'\b(FROM|JOIN)\s+' + re.escape(table_ref) + r'\b'
                if re.search(pattern, sql_str, re.IGNORECASE):
                    # Replace unqualified table references with qualified ones
                    # Use word boundaries to avoid partial matches
                    pattern = r'\b' + re.escape(table_ref) + r'\b'
                    old_sql = sql_str
                    sql_str = re.sub(pattern, f"{schema_name}.{table_ref}", sql_str)
                    logger.debug(f"Qualified table reference: {table_ref} -> {schema_name}.{table_ref}")
                    if old_sql != sql_str:
                        logger.debug(f"SQL changed: {old_sql} -> {sql_str}")
                else:
                    logger.debug(f"Table reference {table_ref} not found in FROM/JOIN clauses, skipping")
        
        return sql_str
    except Exception as e:
        logger.warning(f"Failed to generate qualified SQL for {table_name}: {e}")
        # Fallback to original SQL
        return sql_str


def validate_qualified_sql(original_sql: str, qualified_sql: str, table_name: str) -> None:
    """
    Validate qualified SQL and log warning if length differs significantly.
    
    Args:
        original_sql: Original SQL string
        qualified_sql: Qualified SQL string
        table_name: Table name for logging
    """
    try:
        original_length = len(original_sql)
        qualified_length = len(qualified_sql)
        
        # Calculate percentage difference
        if original_length > 0:
            length_diff_percent = abs(qualified_length - original_length) / original_length * 100
            
            # Log warning if difference is more than 20%
            if length_diff_percent > 20:
                logger.warning(
                    f"Qualified SQL for {table_name} differs significantly from original: "
                    f"original={original_length} chars, qualified={qualified_length} chars "
                    f"({length_diff_percent:.1f}% difference)"
                )
            else:
                logger.debug(
                    f"Qualified SQL for {table_name} validated: "
                    f"original={original_length} chars, qualified={qualified_length} chars "
                    f"({length_diff_percent:.1f}% difference)"
                )
    except Exception as e:
        logger.warning(f"Failed to validate qualified SQL for {table_name}: {e}")
