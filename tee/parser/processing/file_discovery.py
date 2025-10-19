"""
File discovery functionality for finding SQL and Python model files.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any

from ..shared.constants import SUPPORTED_SQL_EXTENSIONS, SUPPORTED_PYTHON_EXTENSIONS, DEFAULT_MODELS_FOLDER
from ..shared.exceptions import FileDiscoveryError

# Configure logging
logger = logging.getLogger(__name__)


class FileDiscovery:
    """Handles discovery of SQL and Python model files."""
    
    def __init__(self, models_folder: Path):
        """
        Initialize the file discovery.
        
        Args:
            models_folder: Path to the models folder
        """
        self.models_folder = models_folder
        self._file_cache: Dict[str, List[Path]] = {}
    
    def discover_sql_files(self) -> List[Path]:
        """
        Discover all SQL files in the models folder.
        
        Returns:
            List of SQL file paths
            
        Raises:
            FileDiscoveryError: If file discovery fails
        """
        try:
            cache_key = "sql_files"
            if cache_key in self._file_cache:
                return self._file_cache[cache_key]
            
            if not self.models_folder.exists():
                raise FileDiscoveryError(f"Models folder not found: {self.models_folder}")
            
            sql_files = []
            for ext in SUPPORTED_SQL_EXTENSIONS:
                sql_files.extend(self.models_folder.rglob(f"*{ext}"))
            
            # Sort for consistent ordering
            sql_files.sort()
            
            # Cache the result
            self._file_cache[cache_key] = sql_files
            
            logger.debug(f"Discovered {len(sql_files)} SQL files")
            return sql_files
            
        except Exception as e:
            if isinstance(e, FileDiscoveryError):
                raise
            raise FileDiscoveryError(f"Failed to discover SQL files: {e}")
    
    def discover_python_files(self) -> List[Path]:
        """
        Discover all Python files in the models folder.
        
        Returns:
            List of Python file paths
            
        Raises:
            FileDiscoveryError: If file discovery fails
        """
        try:
            cache_key = "python_files"
            if cache_key in self._file_cache:
                return self._file_cache[cache_key]
            
            if not self.models_folder.exists():
                raise FileDiscoveryError(f"Models folder not found: {self.models_folder}")
            
            python_files = []
            for ext in SUPPORTED_PYTHON_EXTENSIONS:
                python_files.extend(self.models_folder.rglob(f"*{ext}"))
            
            # Sort for consistent ordering
            python_files.sort()
            
            # Cache the result
            self._file_cache[cache_key] = python_files
            
            logger.debug(f"Discovered {len(python_files)} Python files")
            return python_files
            
        except Exception as e:
            if isinstance(e, FileDiscoveryError):
                raise
            raise FileDiscoveryError(f"Failed to discover Python files: {e}")
    
    def discover_all_files(self) -> Dict[str, List[Path]]:
        """
        Discover all supported files in the models folder.
        
        Returns:
            Dict with 'sql' and 'python' keys containing lists of file paths
            
        Raises:
            FileDiscoveryError: If file discovery fails
        """
        try:
            return {
                "sql": self.discover_sql_files(),
                "python": self.discover_python_files()
            }
        except Exception as e:
            if isinstance(e, FileDiscoveryError):
                raise
            raise FileDiscoveryError(f"Failed to discover files: {e}")
    
    def clear_cache(self) -> None:
        """Clear the file discovery cache."""
        self._file_cache.clear()
        logger.debug("File discovery cache cleared")
    
