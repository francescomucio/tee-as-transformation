"""
Unified connection manager for database adapters.

This module provides a centralized way to manage database connections
for both debug and run commands, eliminating code duplication.
"""

import logging
from typing import Dict, Any, Optional, List
from .executor import ModelExecutor


class ConnectionManager:
    """
    Unified connection manager for database operations.
    
    This class provides a single interface for:
    - Loading project configurations
    - Creating database connections
    - Testing connectivity
    - Managing adapters
    """
    
    def __init__(self, project_folder: str, connection_config: Dict[str, Any], variables: Optional[Dict[str, Any]] = None):
        """
        Initialize the connection manager.
        
        Args:
            project_folder: Path to the project folder
            connection_config: Database connection configuration
            variables: Optional variables for model execution
        """
        self.project_folder = project_folder
        self.connection_config = connection_config
        self.variables = variables or {}
        self.executor = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_executor(self) -> ModelExecutor:
        """Create a ModelExecutor instance."""
        if self.executor is None:
            self.executor = ModelExecutor(self.project_folder, self.connection_config)
        return self.executor
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            executor = self.create_executor()
            return executor.test_connection()
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_database_info(self) -> Optional[Dict[str, Any]]:
        """Get database information."""
        try:
            executor = self.create_executor()
            return executor.get_database_info()
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return None
    
    def get_supported_materializations(self) -> List[str]:
        """Get list of supported materializations."""
        try:
            executor = self.create_executor()
            return executor.list_supported_materializations()
        except Exception as e:
            self.logger.error(f"Failed to get materializations: {e}")
            return []
    
    def execute_models(self, parser, save_analysis: bool = True) -> Dict[str, Any]:
        """
        Execute SQL models.
        
        Args:
            parser: Parser instance with models and execution order
            save_analysis: Whether to save analysis files
            
        Returns:
            Execution results
        """
        try:
            executor = self.create_executor()
            return executor.execute_models(parser, self.variables)
        except Exception as e:
            self.logger.error(f"Model execution failed: {e}")
            raise
    
    def cleanup(self):
        """Clean up resources."""
        if self.executor:
            try:
                self.executor.execution_engine.disconnect()
            except Exception as e:
                self.logger.warning(f"Error during cleanup: {e}")
            self.executor = None
