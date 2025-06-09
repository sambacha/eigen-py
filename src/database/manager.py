"""
Database manager for DuckDB operations.
"""

import duckdb
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schema import DatabaseSchema
from ..core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages DuckDB database operations."""
    
    def __init__(self, db_path: str = "eigenlayer_data.duckdb"):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = Path(db_path)
        self.conn = None
        self._connect()
        
    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.conn = duckdb.connect(str(self.db_path))
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            raise DatabaseError(f"Failed to connect to database: {e}")
    
    def create_schema(self) -> None:
        """Create database schema with all tables and views."""
        try:
            schema = DatabaseSchema(self.conn)
            schema.create_all_tables()
            schema.create_all_views()
            schema.create_all_indexes()
            logger.info("Database schema created successfully")
        except Exception as e:
            raise DatabaseError(f"Schema creation failed: {e}")
    
    def import_strategies(self) -> None:
        """Import strategy reference data."""
        try:
            schema = DatabaseSchema(self.conn)
            schema.import_strategies()
            logger.info("Strategy data imported successfully")
        except Exception as e:
            raise DatabaseError(f"Strategy import failed: {e}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Execute a database query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result
        """
        try:
            if params:
                return self.conn.execute(query, params)
            else:
                return self.conn.execute(query)
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {e}")
    
    def fetch_dataframe(self, query: str, params: Optional[tuple] = None):
        """
        Execute query and return results as pandas DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            result = self.execute_query(query, params)
            return result.df()
        except Exception as e:
            raise DatabaseError(f"DataFrame fetch failed: {e}")
    
    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[tuple]:
        """
        Execute query and return first result.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            First result tuple or None
        """
        try:
            result = self.execute_query(query, params)
            return result.fetchone()
        except Exception as e:
            raise DatabaseError(f"Single fetch failed: {e}")
    
    def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute query and return all results.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result tuples
        """
        try:
            result = self.execute_query(query, params)
            return result.fetchall()
        except Exception as e:
            raise DatabaseError(f"Multiple fetch failed: {e}")
    
    def insert_or_update(
        self, 
        table: str, 
        data: Dict[str, Any], 
        conflict_columns: List[str]
    ) -> None:
        """
        Insert or update data in table.
        
        Args:
            table: Table name
            data: Data dictionary
            conflict_columns: Columns to check for conflicts
        """
        try:
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ', '.join(['?' for _ in values])
            
            # Build conflict resolution
            conflict_clause = ', '.join(conflict_columns)
            update_clause = ', '.join([
                f"{col} = EXCLUDED.{col}" 
                for col in columns if col not in conflict_columns
            ])
            
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_clause}) DO UPDATE SET
                {update_clause}
            """
            
            self.execute_query(query, tuple(values))
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            raise DatabaseError(f"Insert/update failed: {e}")
    
    def bulk_insert(
        self, 
        table: str, 
        data: List[Dict[str, Any]], 
        conflict_columns: Optional[List[str]] = None
    ) -> None:
        """
        Bulk insert data into table.
        
        Args:
            table: Table name
            data: List of data dictionaries
            conflict_columns: Columns to check for conflicts
        """
        if not data:
            return
            
        try:
            for row in data:
                if conflict_columns:
                    self.insert_or_update(table, row, conflict_columns)
                else:
                    columns = list(row.keys())
                    values = list(row.values())
                    placeholders = ', '.join(['?' for _ in values])
                    
                    query = f"""
                        INSERT INTO {table} ({', '.join(columns)})
                        VALUES ({placeholders})
                    """
                    
                    self.execute_query(query, tuple(values))
            
            self.conn.commit()
            logger.info(f"Bulk inserted {len(data)} rows into {table}")
            
        except Exception as e:
            self.conn.rollback()
            raise DatabaseError(f"Bulk insert failed: {e}")
    
    def get_table_count(self, table: str) -> int:
        """Get row count for table."""
        try:
            result = self.fetch_one(f"SELECT COUNT(*) FROM {table}")
            return result[0] if result else 0
        except Exception as e:
            raise DatabaseError(f"Count query failed: {e}")
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        try:
            query = """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """
            result = self.fetch_one(query, (table,))
            return result is not None
        except Exception as e:
            logger.warning(f"Table existence check failed: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            tables = [
                'operators', 'operator_metrics', 'avs', 'avs_metrics',
                'operator_avs_registrations', 'strategies', 
                'operator_strategy_shares', 'avs_strategy_shares'
            ]
            
            stats = {}
            for table in tables:
                if self.table_exists(table):
                    stats[table] = self.get_table_count(table)
                else:
                    stats[table] = 0
            
            return stats
        except Exception as e:
            raise DatabaseError(f"Stats collection failed: {e}")
    
    def vacuum(self) -> None:
        """Optimize database."""
        try:
            self.execute_query("VACUUM")
            logger.info("Database vacuumed")
        except Exception as e:
            logger.warning(f"Vacuum failed: {e}")
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()