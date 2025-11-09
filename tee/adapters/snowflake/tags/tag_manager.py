"""Tag management for Snowflake database objects."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tee.adapters.base.core import DatabaseAdapter

logger = logging.getLogger(__name__)


class TagManager:
    """Manages tag attachment for Snowflake objects."""

    def __init__(self, adapter: "DatabaseAdapter") -> None:
        """
        Initialize the tag manager.

        Args:
            adapter: SnowflakeAdapter instance
        """
        self.adapter = adapter
        self.config = adapter.config
        self.logger = adapter.logger

    @property
    def connection(self) -> Any:
        """Get connection from adapter."""
        return self.adapter.connection

    def attach_tags(
        self, object_type: str, object_name: str, tags: list[str]
    ) -> None:
        """
        Attach tags to a Snowflake database object.

        Snowflake supports tags on tables, views, and other objects using:
        ALTER TABLE/VIEW object_name SET TAG tag_name = 'tag_value'

        For simple string tags, we create/use a generic 'tag' tag and set values.

        Args:
            object_type: Type of object ('TABLE', 'VIEW', etc.)
            object_name: Fully qualified object name (DATABASE.SCHEMA.OBJECT)
            tags: List of tag strings to attach
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        if not tags:
            return

        cursor = self.connection.cursor()
        try:
            # For each tag, we'll use Snowflake's tag system
            # Snowflake tags work as key-value pairs, so we'll create a generic 'tag' tag
            # and set multiple values, or create individual tags for each value

            # Strategy: Create/use a generic 'tee_tags' tag and set comma-separated values
            # OR create individual tags for each tag value
            # We'll use the simpler approach: create a 'tee_tag' tag for each unique tag value

            for tag_value in tags:
                if not tag_value or not isinstance(tag_value, str) or not tag_value.strip():
                    continue

                # Sanitize tag name (Snowflake tag names must be valid identifiers)
                # Use a prefix to avoid conflicts
                sanitized_tag = f"tee_tag_{tag_value.replace(' ', '_').replace('-', '_').lower()}"
                # Truncate if too long (Snowflake has limits)
                if len(sanitized_tag) > 128:
                    sanitized_tag = sanitized_tag[:128]

                try:
                    # Create tag if it doesn't exist (ignore error if it exists)
                    try:
                        create_tag_sql = f"CREATE TAG IF NOT EXISTS {sanitized_tag}"
                        cursor.execute(create_tag_sql)
                        self.logger.debug(f"Created tag: {sanitized_tag}")
                    except Exception:
                        # Tag might already exist, continue
                        pass

                    # Attach tag to object
                    # Snowflake syntax: ALTER TABLE/VIEW object SET TAG tag_name = 'value'
                    # Note: Functions in Snowflake may not support tags directly
                    # If object_type is FUNCTION, we need to include the function signature
                    # For now, we'll try the standard syntax and catch errors
                    if object_type.upper() == "FUNCTION":
                        # Snowflake functions require the full signature for ALTER statements
                        # Since we don't have the signature here, we'll skip tag attachment for functions
                        # and log a debug message
                        self.logger.debug(
                            f"Skipping tag attachment for FUNCTION {object_name}: "
                            f"Snowflake requires function signature for ALTER FUNCTION statements"
                        )
                        continue

                    alter_sql = f"ALTER {object_type} {object_name} SET TAG {sanitized_tag} = '{tag_value.replace("'", "''")}'"
                    cursor.execute(alter_sql)
                    self.logger.debug(f"Attached tag {sanitized_tag}='{tag_value}' to {object_type} {object_name}")

                except Exception as e:
                    self.logger.warning(
                        f"Could not attach tag '{tag_value}' to {object_type} {object_name}: {e}"
                    )
                    # Continue with other tags even if one fails
                    continue

            self.logger.info(f"Attached {len(tags)} tag(s) to {object_type} {object_name}")

        except Exception as e:
            self.logger.warning(f"Error attaching tags to {object_type} {object_name}: {e}")
            # Don't raise - tag attachment is optional
        finally:
            cursor.close()

    def attach_object_tags(
        self, object_type: str, object_name: str, object_tags: dict[str, str]
    ) -> None:
        """
        Attach object tags (key-value pairs) to a Snowflake database object.

        This method handles database-style tags where each tag is a key-value pair,
        like {"sensitivity_tag": "pii", "classification": "public"}.

        Snowflake syntax: ALTER TABLE/VIEW object_name SET TAG tag_name = 'tag_value'

        Args:
            object_type: Type of object ('TABLE', 'VIEW', etc.)
            object_name: Fully qualified object name (DATABASE.SCHEMA.OBJECT)
            object_tags: Dictionary of tag key-value pairs
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        if not object_tags or not isinstance(object_tags, dict):
            return

        cursor = self.connection.cursor()
        try:
            for tag_key, tag_value in object_tags.items():
                if not tag_key or not isinstance(tag_key, str):
                    continue
                if tag_value is None:
                    continue

                # Sanitize tag key (Snowflake tag names must be valid identifiers)
                sanitized_tag_key = tag_key.replace(" ", "_").replace("-", "_")
                # Truncate if too long (Snowflake has limits)
                if len(sanitized_tag_key) > 128:
                    sanitized_tag_key = sanitized_tag_key[:128]

                # Convert tag value to string
                tag_value_str = str(tag_value)

                try:
                    # Create tag if it doesn't exist (ignore error if it exists)
                    try:
                        create_tag_sql = f"CREATE TAG IF NOT EXISTS {sanitized_tag_key}"
                        cursor.execute(create_tag_sql)
                        self.logger.debug(f"Created tag: {sanitized_tag_key}")
                    except Exception:
                        # Tag might already exist, continue
                        pass

                    # Attach tag to object
                    # Snowflake syntax: ALTER TABLE/VIEW object SET TAG tag_name = 'value'
                    # Note: Functions in Snowflake may not support tags directly
                    # If object_type is FUNCTION, we need to include the function signature
                    # For now, we'll try the standard syntax and catch errors
                    if object_type.upper() == "FUNCTION":
                        # Snowflake functions require the full signature for ALTER statements
                        # Since we don't have the signature here, we'll skip tag attachment for functions
                        # and log a debug message
                        self.logger.debug(
                            f"Skipping object tag attachment for FUNCTION {object_name}: "
                            f"Snowflake requires function signature for ALTER FUNCTION statements"
                        )
                        continue

                    escaped_value = tag_value_str.replace("'", "''")
                    alter_sql = f"ALTER {object_type} {object_name} SET TAG {sanitized_tag_key} = '{escaped_value}'"
                    cursor.execute(alter_sql)
                    self.logger.debug(
                        f"Attached object tag {sanitized_tag_key}='{tag_value_str}' to {object_type} {object_name}"
                    )

                except Exception as e:
                    self.logger.warning(
                        f"Could not attach object tag '{tag_key}'='{tag_value}' to {object_type} {object_name}: {e}"
                    )
                    # Continue with other tags even if one fails
                    continue

            self.logger.info(
                f"Attached {len(object_tags)} object tag(s) to {object_type} {object_name}"
            )

        except Exception as e:
            self.logger.warning(
                f"Error attaching object tags to {object_type} {object_name}: {e}"
            )
            # Don't raise - tag attachment is optional
        finally:
            cursor.close()

