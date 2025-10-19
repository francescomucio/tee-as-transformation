"""
Factory for creating parsers based on file type.
"""

from pathlib import Path
from typing import Dict, Type

from .base import BaseParser
from .sql_parser import SQLParser
from .python_parser import PythonParser
from ..shared.constants import SUPPORTED_SQL_EXTENSIONS, SUPPORTED_PYTHON_EXTENSIONS
from ..shared.exceptions import ParserError


class ParserFactory:
    """Factory for creating appropriate parsers based on file type."""
    
    # Registry of parsers by file extension
    _parsers: Dict[str, Type[BaseParser]] = {
        ext: SQLParser for ext in SUPPORTED_SQL_EXTENSIONS
    }
    
    # Python parser for .py files
    _python_parser = PythonParser
    
    @classmethod
    def create_parser(cls, file_path: Path) -> BaseParser:
        """
        Create an appropriate parser for the given file path.
        
        Args:
            file_path: Path to the file to parse
            
        Returns:
            Appropriate parser instance
            
        Raises:
            ParserError: If no parser is available for the file type
        """
        file_extension = file_path.suffix.lower()
        
        # Check if we have a registered parser for this extension
        if file_extension in cls._parsers:
            parser_class = cls._parsers[file_extension]
            return parser_class()
        
        # Special handling for Python files
        if file_extension in SUPPORTED_PYTHON_EXTENSIONS:
            return cls._python_parser()
        
        # No parser available
        raise ParserError(f"No parser available for file type: {file_extension}")
    
    @classmethod
    def is_supported(cls, file_path: Path) -> bool:
        """
        Check if a file type is supported.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if the file type is supported
        """
        file_extension = file_path.suffix.lower()
        return (file_extension in cls._parsers or 
                file_extension in SUPPORTED_PYTHON_EXTENSIONS)
