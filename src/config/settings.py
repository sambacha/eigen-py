"""
Application settings management.
"""

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from .defaults import DEFAULT_CONFIG
from ..core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration."""
    path: str = "eigenlayer_data.duckdb"
    auto_vacuum: bool = True
    timeout: int = 30


@dataclass
class CacheConfig:
    """Cache configuration."""
    directory: str = ".cache"
    default_ttl: int = 3600
    max_size_mb: int = 1000
    cleanup_interval: int = 86400


@dataclass
class NetworkConfig:
    """Network configuration."""
    rpc_url: str = "https://eth.llamarpc.com"
    rpc_timeout: int = 30
    max_retries: int = 3
    rate_limit_delay: float = 0.1


@dataclass
class APIConfig:
    """External API configuration."""
    coingecko_api_key: Optional[str] = None
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    coingecko_timeout: int = 10


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: Optional[str] = None


@dataclass
class ContractConfig:
    """Smart contract configuration."""
    delegation_manager: str = "0x39053D51B77DC0d36036Fc1fCc8Cb819df8Ef37A"
    strategy_manager: str = "0x858646372CC42E1A627fcE94aa7A7033e7CF075A"
    allocation_manager: str = "0xA44151489861Fe9e3055d95adC98FbD462B948e7"
    avs_directory: str = "0x135DDa560e946695d6f155dACaFC6f1F25C1F5AF"


@dataclass
class Settings:
    """Main application settings."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    api: APIConfig = field(default_factory=APIConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    contracts: ContractConfig = field(default_factory=ContractConfig)
    
    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "Settings":
        """
        Load settings from file and environment variables.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Settings instance
        """
        # Start with default configuration
        config = DEFAULT_CONFIG.copy()
        
        # Load from config file if provided
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                    config = cls._deep_merge(config, file_config)
                logger.info(f"Loaded configuration from {config_path}")
            except Exception as e:
                raise ConfigurationError(f"Failed to load config file: {e}")
        
        # Override with environment variables
        config = cls._load_from_env(config)
        
        # Create settings instance
        try:
            return cls(
                database=DatabaseConfig(**config.get("database", {})),
                cache=CacheConfig(**config.get("cache", {})),
                network=NetworkConfig(**config.get("network", {})),
                api=APIConfig(**config.get("api", {})),
                logging=LoggingConfig(**config.get("logging", {})),
                contracts=ContractConfig(**config.get("contracts", {}))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid configuration: {e}")
    
    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Settings._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    @staticmethod
    def _load_from_env(config: Dict[str, Any]) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        env_mappings = {
            # Database
            "EIGENLAYER_DB_PATH": ("database", "path"),
            "EIGENLAYER_DB_TIMEOUT": ("database", "timeout"),
            
            # Cache
            "EIGENLAYER_CACHE_DIR": ("cache", "directory"),
            "EIGENLAYER_CACHE_TTL": ("cache", "default_ttl"),
            "EIGENLAYER_CACHE_MAX_SIZE": ("cache", "max_size_mb"),
            
            # Network
            "ETH_RPC_URL": ("network", "rpc_url"),
            "EIGENLAYER_RPC_TIMEOUT": ("network", "rpc_timeout"),
            "EIGENLAYER_MAX_RETRIES": ("network", "max_retries"),
            
            # API
            "COINGECKO_API_KEY": ("api", "coingecko_api_key"),
            "COINGECKO_BASE_URL": ("api", "coingecko_base_url"),
            
            # Logging
            "EIGENLAYER_LOG_LEVEL": ("logging", "level"),
            "EIGENLAYER_LOG_FILE": ("logging", "file"),
        }
        
        for env_var, (section, key) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                if section not in config:
                    config[section] = {}
                
                # Type conversion
                if key in ["timeout", "default_ttl", "max_size_mb", "rpc_timeout", "max_retries"]:
                    try:
                        value = int(value)
                    except ValueError:
                        logger.warning(f"Invalid integer value for {env_var}: {value}")
                        continue
                elif key in ["rate_limit_delay"]:
                    try:
                        value = float(value)
                    except ValueError:
                        logger.warning(f"Invalid float value for {env_var}: {value}")
                        continue
                elif key in ["auto_vacuum"]:
                    value = value.lower() in ("true", "1", "yes", "on")
                
                config[section][key] = value
        
        return config
    
    def save(self, config_path: str) -> None:
        """
        Save current settings to file.
        
        Args:
            config_path: Path to save configuration file
        """
        config = {
            "database": {
                "path": self.database.path,
                "auto_vacuum": self.database.auto_vacuum,
                "timeout": self.database.timeout
            },
            "cache": {
                "directory": self.cache.directory,
                "default_ttl": self.cache.default_ttl,
                "max_size_mb": self.cache.max_size_mb,
                "cleanup_interval": self.cache.cleanup_interval
            },
            "network": {
                "rpc_url": self.network.rpc_url,
                "rpc_timeout": self.network.rpc_timeout,
                "max_retries": self.network.max_retries,
                "rate_limit_delay": self.network.rate_limit_delay
            },
            "api": {
                "coingecko_api_key": self.api.coingecko_api_key,
                "coingecko_base_url": self.api.coingecko_base_url,
                "coingecko_timeout": self.api.coingecko_timeout
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "file": self.logging.file
            },
            "contracts": {
                "delegation_manager": self.contracts.delegation_manager,
                "strategy_manager": self.contracts.strategy_manager,
                "allocation_manager": self.contracts.allocation_manager,
                "avs_directory": self.contracts.avs_directory
            }
        }
        
        try:
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info(f"Configuration saved to {config_path}")
        except Exception as e:
            raise ConfigurationError(f"Failed to save config: {e}")
    
    def setup_logging(self) -> None:
        """Setup logging based on configuration."""
        level = getattr(logging, self.logging.level.upper(), logging.INFO)
        
        handlers = []
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(self.logging.format))
        handlers.append(console_handler)
        
        # File handler if specified
        if self.logging.file:
            file_handler = logging.FileHandler(self.logging.file)
            file_handler.setFormatter(logging.Formatter(self.logging.format))
            handlers.append(file_handler)
        
        # Configure root logger
        logging.basicConfig(
            level=level,
            handlers=handlers,
            force=True
        )
        
        logger.info(f"Logging configured: level={self.logging.level}")
    
    def validate(self) -> None:
        """Validate configuration settings."""
        errors = []
        
        # Database validation
        if not self.database.path:
            errors.append("Database path is required")
        
        # Cache validation
        if self.cache.default_ttl <= 0:
            errors.append("Cache TTL must be positive")
        
        if self.cache.max_size_mb <= 0:
            errors.append("Cache max size must be positive")
        
        # Network validation
        if not self.network.rpc_url:
            errors.append("RPC URL is required")
        
        if self.network.rpc_timeout <= 0:
            errors.append("RPC timeout must be positive")
        
        # Contract validation
        required_contracts = [
            self.contracts.delegation_manager,
            self.contracts.strategy_manager,
            self.contracts.avs_directory
        ]
        
        for contract in required_contracts:
            if not contract or len(contract) != 42 or not contract.startswith("0x"):
                errors.append(f"Invalid contract address: {contract}")
        
        if errors:
            raise ConfigurationError(f"Configuration validation failed: {'; '.join(errors)}")