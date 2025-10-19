"""
Tee Executor

Handles the complete workflow of parsing and executing SQL models based on project configuration.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, Union, TYPE_CHECKING
from .parser import ProjectParser
from .engine import ModelExecutor

if TYPE_CHECKING:
    from tee.adapters import AdapterConfig


def execute_models(project_folder: str, connection_config: Union[Dict[str, Any], AdapterConfig], 
                  save_analysis: bool = True, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute SQL models by parsing them and running them in dependency order.
    
    This function handles the complete workflow:
    1. Parse SQL models from the project folder
    2. Build dependency graph and determine execution order
    3. Execute models using the execution engine
    4. Optionally save analysis files
    
    Args:
        project_folder: Path to the project folder containing SQL models
        connection_config: Database connection configuration
        save_analysis: Whether to save parsing analysis to files
        
    Returns:
        Dictionary containing execution results and analysis info
    """
    logger = logging.getLogger(__name__)
    
    # Keep raw config dict for adapter validation
    # Adapters will handle their own validation and config creation
    
    print("\n" + "="*50)
    print("TEE: PARSING AND EXECUTING SQL MODELS")
    print("="*50)
    
    # Step 1: Parse SQL models
    logger.info("Parsing SQL models...")
    parser = ProjectParser(project_folder, connection_config, variables)
    
    print("\nCollecting and parsing SQL models...")
    parsed_models = parser.collect_models()
    print(f"Found {len(parsed_models)} SQL files")
    
    # Step 2: Build dependency graph and get execution order
    print("\nBuilding dependency graph...")
    graph = parser.build_dependency_graph()
    execution_order = parser.get_execution_order()
    print(f"Found {len(graph['nodes'])} tables")
    print(f"Execution order: {' -> '.join(execution_order)}")
    
    # Step 3: Execute models
    print("\n" + "="*50)
    print("EXECUTING SQL MODELS")
    print("="*50)
    
    model_executor = ModelExecutor(project_folder, connection_config)
    
    try:
        # Execute models using the executor
        results = model_executor.execute_models(parser, variables)
        
        # Step 4: Save analysis files if requested (after execution to include qualified SQL)
        if save_analysis:
            parser.save_to_json()
            print("Analysis files saved to output folder")
        
        # Print results
        print("\nExecution Results:")
        print(f"  Successfully executed: {len(results['executed_tables'])} tables")
        print(f"  Failed: {len(results['failed_tables'])} tables")
        
        if results['executed_tables']:
            print("\nSuccessfully executed tables:")
            for table in results['executed_tables']:
                table_info = results['table_info'].get(table, {})
                row_count = table_info.get('row_count', 0)
                print(f"  - {table}: {row_count} rows")
        
        if results['failed_tables']:
            print("\nFailed tables:")
            for failure in results['failed_tables']:
                print(f"  - {failure['table']}: {failure['error']}")
        
        # Get database info
        try:
            db_info = model_executor.get_database_info()
            if db_info:
                print("\nDatabase Info:")
                print(f"  Type: {db_info.get('connection_type', 'Unknown')}")
                print(f"  Connected: {db_info.get('is_connected', False)}")
        except Exception as e:
            print(f"\nDatabase Info: Error getting info - {e}")
        
        # Add analysis info to results
        results['analysis'] = {
            'total_models': len(parsed_models),
            'total_tables': len(graph['nodes']),
            'execution_order': execution_order,
            'dependency_graph': graph
        }
        
        return results
        
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        print(f"Error during execution: {e}")
        raise


def parse_models_only(project_folder: str, connection_config: Union[Dict[str, Any], AdapterConfig], 
                     variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Parse SQL models and build dependency graph without executing them.
    
    Args:
        project_folder: Path to the project folder containing SQL models
        connection_config: Database connection configuration
        
    Returns:
        Dictionary containing parsing results and dependency information
    """
    logger = logging.getLogger(__name__)
    
    # Keep raw config dict for adapter validation
    # Adapters will handle their own validation and config creation
    
    print("\n" + "="*50)
    print("TEE: PARSING SQL MODELS")
    print("="*50)
    
    # Parse SQL models
    logger.info("Parsing SQL models...")
    parser = ProjectParser(project_folder, connection_config, variables)
    
    print("\nCollecting and parsing SQL models...")
    parsed_models = parser.collect_models()
    print(f"Found {len(parsed_models)} SQL files")
    
    # Build dependency graph
    print("\nBuilding dependency graph...")
    graph = parser.build_dependency_graph()
    execution_order = parser.get_execution_order()
    print(f"Found {len(graph['nodes'])} tables")
    print(f"Execution order: {' -> '.join(execution_order)}")
    
    # Save analysis files
    parser.save_to_json()
    print("Analysis files saved to output folder")
    
    return {
        'parsed_models': parsed_models,
        'dependency_graph': graph,
        'execution_order': execution_order,
        'total_models': len(parsed_models),
        'total_tables': len(graph['nodes'])
    }
