"""
Database configuration management.

This module handles loading database configurations from pyproject.toml
and environment variables with proper precedence and validation.
"""

import os
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
import toml

from ..adapters.base import AdapterConfig


class DatabaseConfigManager:
    """Manages database configurations from multiple sources."""
    
    def __init__(self, project_root: Optional[str] = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_config(self, config_name: str = "default") -> AdapterConfig:
        """
        Load database configuration from pyproject.toml and environment variables.
        
        Args:
            config_name: Name of the configuration to load (default: "default")
            
        Returns:
            AdapterConfig object with merged configuration
            
        Raises:
            ValueError: If configuration is invalid or missing
        """
        # Load from pyproject.toml
        toml_config = self._load_toml_config(config_name)
        
        # Load from environment variables
        env_config = self._load_env_config()
        
        # Merge configurations (env vars override toml)
        merged_config = self._merge_configs(toml_config, env_config)
        
        # Validate and create AdapterConfig
        return self._create_adapter_config(merged_config)
    
    def list_available_configs(self) -> List[str]:
        """List available configuration names from pyproject.toml."""
        toml_file = self.project_root / "pyproject.toml"
        if not toml_file.exists():
            return []
        
        try:
            with open(toml_file, 'r') as f:
                data = toml.load(f)
            
            # Look for [tool.tee.database] or [tool.tee.databases]
            tee_config = data.get("tool", {}).get("tee", {})
            databases = tee_config.get("databases", {})
            
            if isinstance(databases, dict):
                return list(databases.keys())
            else:
                return []
                
        except Exception as e:
            self.logger.warning(f"Could not read pyproject.toml: {e}")
            return []
    
    def _load_toml_config(self, config_name: str) -> Dict[str, Any]:
        """Load configuration from pyproject.toml."""
        toml_file = self.project_root / "pyproject.toml"
        if not toml_file.exists():
            self.logger.debug("No pyproject.toml found")
            return {}
        
        try:
            with open(toml_file, 'r') as f:
                data = toml.load(f)
            
            # Look for [tool.tee.database] or [tool.tee.databases]
            tee_config = data.get("tool", {}).get("tee", {})
            
            # Check for single database config
            if "database" in tee_config:
                return tee_config["database"]
            
            # Check for multiple database configs
            databases = tee_config.get("databases", {})
            if isinstance(databases, dict) and config_name in databases:
                return databases[config_name]
            
            self.logger.debug(f"No database configuration '{config_name}' found in pyproject.toml")
            return {}
            
        except Exception as e:
            self.logger.warning(f"Could not read pyproject.toml: {e}")
            return {}
    
    def _load_env_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        env_config = {}
        
        # Map environment variables to config keys
        env_mappings = {
            "TEE_DB_TYPE": "type",
            "TEE_DB_HOST": "host",
            "TEE_DB_PORT": "port",
            "TEE_DB_DATABASE": "database",
            "TEE_DB_USER": "user",
            "TEE_DB_PASSWORD": "password",
            "TEE_DB_PATH": "path",
            "TEE_DB_SCHEMA": "schema",
            "TEE_DB_WAREHOUSE": "warehouse",
            "TEE_DB_ROLE": "role",
            "TEE_DB_PROJECT": "project",
            "TEE_DB_SOURCE_DIALECT": "source_dialect",
            "TEE_DB_TARGET_DIALECT": "target_dialect",
        }
        
        for env_var, config_key in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert port to int if it's a number
                if config_key == "port" and value.isdigit():
                    env_config[config_key] = int(value)
                else:
                    env_config[config_key] = value
        
        return env_config
    
    def _merge_configs(self, toml_config: Dict[str, Any], env_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge TOML and environment configurations."""
        merged = toml_config.copy()
        merged.update(env_config)
        return merged
    
    def _create_adapter_config(self, config_dict: Dict[str, Any]) -> AdapterConfig:
        """Create AdapterConfig from dictionary."""
        if not config_dict:
            raise ValueError("No database configuration found")
        
        # Extract required fields
        db_type = config_dict.get("type")
        if not db_type:
            raise ValueError("Database type is required")
        
        # Create AdapterConfig
        return AdapterConfig(
            type=db_type,
            host=config_dict.get("host"),
            port=config_dict.get("port"),
            database=config_dict.get("database"),
            user=config_dict.get("user"),
            password=config_dict.get("password"),
            path=config_dict.get("path"),
            source_dialect=config_dict.get("source_dialect"),
            target_dialect=config_dict.get("target_dialect"),
            connection_timeout=config_dict.get("connection_timeout", 30),
            query_timeout=config_dict.get("query_timeout", 300),
            schema=config_dict.get("schema"),
            warehouse=config_dict.get("warehouse"),
            role=config_dict.get("role"),
            project=config_dict.get("project"),
            extra=config_dict.get("extra"),
        )


def load_database_config(config_name: str = "default", project_root: Optional[str] = None) -> AdapterConfig:
    """
    Convenience function to load database configuration.
    
    Args:
        config_name: Name of the configuration to load
        project_root: Project root directory (defaults to current directory)
        
    Returns:
        AdapterConfig object
    """
    manager = DatabaseConfigManager(project_root)
    return manager.load_config(config_name)
