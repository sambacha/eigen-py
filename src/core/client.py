"""
Main client interface for EigenLayer analysis.
"""

from typing import List, Optional, Dict, Any
import logging
from pathlib import Path

from ..database.manager import DatabaseManager
from ..query.engine import QueryEngine
from ..cache.manager import CacheManager
from ..config.settings import Settings
from .models import OperatorStats, AVSMetrics, SystemMetrics
from .exceptions import EigenLayerError

logger = logging.getLogger(__name__)


class EigenLayerClient:
    """
    Main client interface for EigenLayer analysis.
    
    Provides a high-level API for querying operators, AVS, and strategies
    with built-in caching and database management.
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        db_path: Optional[str] = None,
        cache_dir: Optional[str] = None
    ):
        """
        Initialize the EigenLayer client.
        
        Args:
            config_path: Path to configuration file
            db_path: Path to database file
            cache_dir: Path to cache directory
        """
        # Load configuration
        self.settings = Settings.load(config_path)
        
        # Initialize components
        self.db_manager = DatabaseManager(
            db_path or self.settings.database.path
        )
        self.cache_manager = CacheManager(
            cache_dir or self.settings.cache.directory
        )
        self.query_engine = QueryEngine(
            db_manager=self.db_manager,
            cache_manager=self.cache_manager,
            settings=self.settings
        )
        
        logger.info(f"EigenLayer client initialized")
        logger.info(f"Database: {self.db_manager.db_path}")
        logger.info(f"Cache: {self.cache_manager.cache_dir}")
    
    def setup_database(self) -> None:
        """Initialize the database schema and import base data."""
        try:
            self.db_manager.create_schema()
            self.db_manager.import_strategies()
            logger.info("Database setup completed")
        except Exception as e:
            raise EigenLayerError(f"Database setup failed: {e}")
    
    def import_data(self, force_refresh: bool = False) -> None:
        """
        Import operator and AVS data from available sources.
        
        Args:
            force_refresh: Force refresh of cached data
        """
        try:
            from ..database.importers import DataImporter
            importer = DataImporter(self.db_manager)
            importer.import_all_data(force_refresh=force_refresh)
            logger.info("Data import completed")
        except Exception as e:
            raise EigenLayerError(f"Data import failed: {e}")
    
    # Operator methods
    def get_operator(self, address: str) -> Optional[OperatorStats]:
        """Get detailed information for a specific operator."""
        return self.query_engine.get_operator_by_address(address)
    
    def get_top_operators(self, limit: int = 10) -> List[OperatorStats]:
        """Get top operators by TVL."""
        return self.query_engine.get_top_operators(limit)
    
    def get_all_operators(self) -> List[OperatorStats]:
        """Get all operators with their metrics."""
        return self.query_engine.get_all_operators()
    
    def search_operators(self, query: str) -> List[OperatorStats]:
        """Search operators by name or description."""
        return self.query_engine.search_operators(query)
    
    # AVS methods
    def get_avs(self, address: str) -> Optional[AVSMetrics]:
        """Get detailed information for a specific AVS."""
        return self.query_engine.get_avs_by_address(address)
    
    def get_all_avs(self) -> List[AVSMetrics]:
        """Get all AVS with their metrics."""
        return self.query_engine.get_all_avs()
    
    def get_operator_avs(self, operator_address: str) -> List[AVSMetrics]:
        """Get all AVS that an operator is registered with."""
        return self.query_engine.get_operator_avs(operator_address)
    
    # System metrics
    def get_system_metrics(self) -> SystemMetrics:
        """Get overall system metrics."""
        return self.query_engine.get_system_metrics()
    
    def get_strategy_distribution(self) -> Dict[str, Any]:
        """Get strategy distribution across operators."""
        return self.query_engine.get_strategy_distribution()
    
    def get_concentration_metrics(self) -> Dict[str, Any]:
        """Get market concentration metrics."""
        return self.query_engine.get_concentration_metrics()
    
    # Analysis methods
    def analyze_operator_efficiency(self) -> Dict[str, Any]:
        """Analyze operator efficiency patterns."""
        return self.query_engine.analyze_operator_efficiency()
    
    def analyze_avs_network(self) -> Dict[str, Any]:
        """Analyze AVS network effects and overlap."""
        return self.query_engine.analyze_avs_network()
    
    def analyze_strategy_risk(self) -> Dict[str, Any]:
        """Analyze strategy concentration risk."""
        return self.query_engine.analyze_strategy_risk()
    
    # Cache management
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self.cache_manager.get_stats()
    
    def clear_cache(self, cache_type: Optional[str] = None) -> None:
        """Clear cache files."""
        self.cache_manager.clear_cache(cache_type)
        logger.info(f"Cache cleared: {cache_type or 'all'}")
    
    # Export methods
    def export_operators(self, file_path: str, format: str = "json") -> None:
        """Export operator data to file."""
        data = self.get_all_operators()
        self._export_data(data, file_path, format)
    
    def export_avs(self, file_path: str, format: str = "json") -> None:
        """Export AVS data to file."""
        data = self.get_all_avs()
        self._export_data(data, file_path, format)
    
    def export_system_report(self, file_path: str) -> None:
        """Export comprehensive system report."""
        report = {
            "system_metrics": self.get_system_metrics(),
            "top_operators": self.get_top_operators(20),
            "strategy_distribution": self.get_strategy_distribution(),
            "concentration_metrics": self.get_concentration_metrics(),
            "cache_stats": self.get_cache_stats()
        }
        self._export_data(report, file_path, "json")
    
    def _export_data(self, data: Any, file_path: str, format: str) -> None:
        """Export data to file in specified format."""
        import json
        import pandas as pd
        from datetime import datetime
        
        # Convert dataclasses to dict for serialization
        if hasattr(data, '__dict__'):
            data = data.__dict__
        elif isinstance(data, list) and data and hasattr(data[0], '__dict__'):
            data = [item.__dict__ for item in data]
        
        file_path = Path(file_path)
        
        if format.lower() == "json":
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        elif format.lower() == "csv":
            df = pd.DataFrame(data)
            df.to_csv(file_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Data exported to {file_path}")
    
    def close(self) -> None:
        """Close database connections and cleanup resources."""
        self.db_manager.close()
        logger.info("EigenLayer client closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()