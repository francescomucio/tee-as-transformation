"""
OTS Transformer - Transforms parsed models to OTS Module format.

This module implements the transformation from tee's internal parsed model format
to the Open Transformation Specification (OTS) Module format.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

from tee.typing.metadata import OTSModule
from tee.parser.shared.types import ParsedModel, ParsedFunction
from .utils import infer_sql_dialect, group_models_by_schema, group_functions_by_schema
from .transformers import ModelTransformer, FunctionTransformer
from .builders import ModuleBuilder

# Configure logging
logger = logging.getLogger(__name__)


class OTSTransformer:
    """Transforms parsed models to OTS Module format."""

    def __init__(self, project_config: Dict[str, Any]):
        """
        Initialize the OTS transformer.

        Args:
            project_config: Project configuration from project.toml
        """
        self.project_config = project_config
        self.database = project_config.get("project_folder", "unknown")
        self.sql_dialect = infer_sql_dialect(project_config)

        # Initialize components
        self.model_transformer = ModelTransformer(project_config, self.sql_dialect)
        self.function_transformer = FunctionTransformer(project_config)
        self.module_builder = ModuleBuilder(project_config, self.database, self.sql_dialect)

        logger.debug(
            f"Initialized OTS transformer: database={self.database}, dialect={self.sql_dialect}"
        )

    def transform_to_ots_modules(
        self,
        parsed_models: Dict[str, ParsedModel],
        parsed_functions: Optional[Dict[str, ParsedFunction]] = None,
        test_library_path: Optional[Path] = None,
    ) -> Dict[str, OTSModule]:
        """
        Transform parsed models and functions into OTS Module(s).

        Groups models and functions by schema and creates one module per schema.

        Args:
            parsed_models: Dictionary of parsed models
            parsed_functions: Optional dictionary of parsed functions
            test_library_path: Optional path to test library

        Returns:
            Dictionary mapping module_name to OTS Module
        """
        # Group models and functions by schema
        models_by_schema = group_models_by_schema(parsed_models)
        functions_by_schema = group_functions_by_schema(parsed_functions or {})

        # Build modules using module builder
        return self.module_builder.build_modules(
            models_by_schema,
            functions_by_schema,
            test_library_path,
            self.model_transformer,
            self.function_transformer,
        )

