"""
Adapter registry for managing database adapters.

This module provides a registry system for discovering and managing
database adapters with automatic registration and factory methods.
"""

from typing import Dict, Type, Optional, List
import logging
from .base import DatabaseAdapter, AdapterConfig


class AdapterRegistry:
    """Registry for managing database adapters."""
    
    def __init__(self):
        self._adapters: Dict[str, Type[DatabaseAdapter]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register(self, adapter_type: str, adapter_class: Type[DatabaseAdapter]) -> None:
        """
        Register a database adapter.
        
        Args:
            adapter_type: Database type identifier (e.g., 'duckdb', 'snowflake')
            adapter_class: Adapter class that implements DatabaseAdapter
        """
        self._adapters[adapter_type.lower()] = adapter_class
        self.logger.debug(f"Registered adapter: {adapter_type} -> {adapter_class.__name__}")
    
    def get_adapter_class(self, adapter_type: str) -> Optional[Type[DatabaseAdapter]]:
        """
        Get adapter class for a database type.
        
        Args:
            adapter_type: Database type identifier
            
        Returns:
            Adapter class or None if not found
        """
        return self._adapters.get(adapter_type.lower())
    
    def create_adapter(self, config: AdapterConfig) -> DatabaseAdapter:
        """
        Create an adapter instance from configuration.
        
        Args:
            config: Adapter configuration
            
        Returns:
            Configured adapter instance
            
        Raises:
            ValueError: If adapter type is not supported
        """
        adapter_class = self.get_adapter_class(config.type)
        if not adapter_class:
            supported_types = list(self._adapters.keys())
            raise ValueError(
                f"Unsupported database type: {config.type}. "
                f"Supported types: {supported_types}"
            )
        
        return adapter_class(config)
    
    def list_adapters(self) -> List[str]:
        """Get list of registered adapter types."""
        return list(self._adapters.keys())
    
    def is_supported(self, adapter_type: str) -> bool:
        """Check if an adapter type is supported."""
        return adapter_type.lower() in self._adapters


# Global registry instance
_registry = AdapterRegistry()


def register_adapter(adapter_type: str, adapter_class: Type[DatabaseAdapter]) -> None:
    """Register an adapter with the global registry."""
    _registry.register(adapter_type, adapter_class)


def get_adapter(config: AdapterConfig) -> DatabaseAdapter:
    """
    Get an adapter instance from configuration.
    
    Args:
        config: Adapter configuration
        
    Returns:
        Configured adapter instance
    """
    return _registry.create_adapter(config)


def list_available_adapters() -> List[str]:
    """Get list of available adapter types."""
    return _registry.list_adapters()


def is_adapter_supported(adapter_type: str) -> bool:
    """Check if an adapter type is supported."""
    return _registry.is_supported(adapter_type)
