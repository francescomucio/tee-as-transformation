#!/usr/bin/env python3
import re

def test_regex():
    query = "select id from my_first_table where id = 1;"
    schema_name = "my_schema"
    
    print(f"Original query: {query}")
    print(f"Schema name: {schema_name}")
    
    # Pattern to match unqualified table names after FROM/JOIN
    pattern = r'\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*)?\b'
    
    def replace_unqualified_table(match):
        keyword = match.group(1)  # FROM or JOIN
        table_name = match.group(2)  # table name
        full_match = match.group(0)  # entire match
        
        print(f"Match: '{full_match}' -> keyword: '{keyword}', table_name: '{table_name}'")
        
        # Skip if it's a reserved word
        if table_name.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'FROM', 'JOIN']:
            print(f"  -> Skipping reserved word: {table_name}")
            return full_match
        
        # Skip if it's already schema-qualified (contains a dot)
        if '.' in table_name:
            print(f"  -> Skipping already qualified: {table_name}")
            return full_match
        
        # Replace the table name with schema-qualified version
        result = full_match.replace(table_name, f"{schema_name}.{table_name}")
        print(f"  -> Replacing: {full_match} -> {result}")
        return result
    
    resolved_query = re.sub(pattern, replace_unqualified_table, query, flags=re.IGNORECASE)
    print(f"Resolved query: {resolved_query}")

if __name__ == "__main__":
    test_regex()

