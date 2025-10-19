"""
Incremental state management for tracking model execution state.

This module provides functionality to track incremental model state including:
- Last processed values (timestamps, IDs)
- Model definitions (SQL hash for change detection)
- Execution metadata
"""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, asdict

import duckdb

logger = logging.getLogger(__name__)


@dataclass
class IncrementalState:
    """State information for an incremental model."""
    model_name: str
    strategy: str
    last_processed_value: Optional[str] = None
    last_run_timestamp: Optional[str] = None
    sqlglot_hash: Optional[str] = None
    config_hash: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class IncrementalStateManager:
    """Manages incremental model state using DuckDB."""
    
    def __init__(self, state_database_path: Optional[str] = None):
        """
        Initialize the state manager.
        
        Args:
            state_database_path: Path to the state database. If None, uses default location.
        """
        if state_database_path is None:
            # Default to tee_state.db in the current directory
            state_database_path = "tee_state.db"
        
        self.state_database_path = Path(state_database_path)
        self.connection = None
        self._ensure_state_table()
    
    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self.connection is None:
            self.connection = duckdb.connect(str(self.state_database_path))
        return self.connection
    
    def _ensure_state_table(self) -> None:
        """Create the state table if it doesn't exist."""
        conn = self._get_connection()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tee_incremental_state (
            model_name VARCHAR PRIMARY KEY,
            strategy VARCHAR NOT NULL,
            last_processed_value VARCHAR,
            last_run_timestamp VARCHAR,
            sqlglot_hash VARCHAR,
            config_hash VARCHAR,
            created_at VARCHAR,
            updated_at VARCHAR
        )
        """
        
        conn.execute(create_table_sql)
        logger.debug("Ensured incremental state table exists")
    
    def get_state(self, model_name: str) -> Optional[IncrementalState]:
        """
        Get the current state for a model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            IncrementalState object or None if not found
        """
        conn = self._get_connection()
        logger.info(f"Getting state for {model_name} from database: {self.state_database_path}")
        
        query = """
        SELECT * FROM tee_incremental_state 
        WHERE model_name = ?
        """
        
        result = conn.execute(query, [model_name]).fetchone()
        logger.info(f"Raw database result for {model_name}: {result}")
        
        if result is None:
            return None
        
        return IncrementalState(
            model_name=result[0],
            strategy=result[1],
            last_processed_value=result[2],
            last_run_timestamp=result[3],
            sqlglot_hash=result[4],
            config_hash=result[5],
            created_at=result[6],
            updated_at=result[7]
        )
    
    def save_state(self, state: IncrementalState) -> None:
        """
        Save or update the state for a model.
        
        Args:
            state: IncrementalState object to save
        """
        conn = self._get_connection()
        
        # Check if model exists by querying directly
        check_sql = "SELECT COUNT(*) FROM tee_incremental_state WHERE model_name = ?"
        count = conn.execute(check_sql, [state.model_name]).fetchone()[0]
        logger.info(f"Model {state.model_name} exists: {count > 0}")
        
        if count == 0:
            logger.info(f"No existing state found, inserting new state for {state.model_name}")
            # Insert new state
            insert_sql = """
            INSERT INTO tee_incremental_state 
            (model_name, strategy, last_processed_value, last_run_timestamp, 
             sqlglot_hash, config_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            now = datetime.utcnow().isoformat()
            conn.execute(insert_sql, [
                state.model_name,
                state.strategy,
                state.last_processed_value,
                state.last_run_timestamp,
                state.sqlglot_hash,
                state.config_hash,
                now,  # created_at
                now   # updated_at
            ])
        else:
            # Update existing state
            update_sql = """
            UPDATE tee_incremental_state 
            SET strategy = ?, last_processed_value = ?, last_run_timestamp = ?,
                sqlglot_hash = ?, config_hash = ?, updated_at = ?
            WHERE model_name = ?
            """
            
            now = datetime.utcnow().isoformat()
            logger.info(f"Updating state for {state.model_name} with last_processed_value: {state.last_processed_value}")
            result = conn.execute(update_sql, [
                state.strategy,
                state.last_processed_value,
                state.last_run_timestamp,
                state.sqlglot_hash,
                state.config_hash,
                now,
                state.model_name
            ])
            logger.info(f"UPDATE result: {result.fetchall()}")
        
        # Commit the transaction
        conn.commit()
        logger.debug(f"Saved state for model: {state.model_name}")
    
    def update_processed_value(self, model_name: str, value: str) -> None:
        """
        Update the last processed value for a model.
        
        Args:
            model_name: Name of the model
            value: Last processed value (timestamp, ID, etc.)
        """
        state = self.get_state(model_name)
        if state is None:
            logger.warning(f"Cannot update processed value for unknown model: {model_name}")
            return
        
        logger.info(f"Updating processed value for {model_name}: {state.last_processed_value} -> {value}")
        state.last_processed_value = value
        state.last_run_timestamp = datetime.utcnow().isoformat()
        self.save_state(state)
        logger.info(f"State saved for {model_name}: {state}")
    
    def has_model_changed(self, model_name: str, current_sql_hash: str, current_config_hash: str) -> bool:
        """
        Check if a model definition has changed since last run.
        
        Args:
            model_name: Name of the model
            current_sql_hash: Current SQL hash
            current_config_hash: Current configuration hash
            
        Returns:
            True if model has changed, False otherwise
        """
        state = self.get_state(model_name)
        if state is None:
            return True  # New model, consider it changed
        
        return (state.sqlglot_hash != current_sql_hash or 
                state.config_hash != current_config_hash)
    
    def should_run_incremental(self, model_name: str, current_sql_hash: str, current_config_hash: str) -> bool:
        """
        Determine if a model should run incrementally or as a full load.
        
        Args:
            model_name: Name of the model
            current_sql_hash: Current SQL hash
            current_config_hash: Current configuration hash
            
        Returns:
            True if should run incrementally, False for full load
        """
        state = self.get_state(model_name)
        logger.info(f"State for {model_name}: {state}")
        logger.info(f"Current SQL hash: {current_sql_hash}")
        logger.info(f"Current config hash: {current_config_hash}")
        
        # If no state exists, run as full load
        if state is None:
            logger.info(f"No state exists for {model_name}, running full load")
            return False
        
        # If model definition changed, run as full load
        if self.has_model_changed(model_name, current_sql_hash, current_config_hash):
            return False
        
        # If no last processed value, run as full load
        if not state.last_processed_value:
            return False
        
        return True
    
    def compute_sql_hash(self, sql_query: str) -> str:
        """
        Compute hash for SQL query.
        
        Args:
            sql_query: SQL query string
            
        Returns:
            Hash string
        """
        return hashlib.sha256(sql_query.encode('utf-8')).hexdigest()
    
    def compute_config_hash(self, config: Dict[str, Any]) -> str:
        """
        Compute hash for configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Hash string
        """
        # Sort keys to ensure consistent hashing
        sorted_config = json.dumps(config, sort_keys=True, default=str)
        return hashlib.sha256(sorted_config.encode('utf-8')).hexdigest()
    
    def list_models(self) -> List[str]:
        """
        List all models with state.
        
        Returns:
            List of model names
        """
        conn = self._get_connection()
        
        query = "SELECT model_name FROM tee_incremental_state ORDER BY model_name"
        result = conn.execute(query).fetchall()
        
        return [row[0] for row in result]
    
    def delete_state(self, model_name: str) -> None:
        """
        Delete state for a model.
        
        Args:
            model_name: Name of the model
        """
        conn = self._get_connection()
        
        delete_sql = "DELETE FROM tee_incremental_state WHERE model_name = ?"
        conn.execute(delete_sql, [model_name])
        
        logger.debug(f"Deleted state for model: {model_name}")
    
    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
