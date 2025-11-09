"""
Input parsers for external formats (OTS modules, etc.)
"""

from .ots_reader import OTSModuleReader, OTSModuleReaderError
from .ots_converter import OTSConverter, OTSConverterError
from .ots_integration import (
    load_ots_modules,
    merge_ots_with_parsed_models,
    merge_ots_with_parsed_functions,
)
from .ots_validator import validate_ots_module_location, OTSValidationError

__all__ = [
    "OTSModuleReader",
    "OTSModuleReaderError",
    "OTSConverter",
    "OTSConverterError",
    "load_ots_modules",
    "merge_ots_with_parsed_models",
    "merge_ots_with_parsed_functions",
    "validate_ots_module_location",
    "OTSValidationError",
]

