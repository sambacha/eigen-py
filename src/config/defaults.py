"""
Default configuration values.
"""

DEFAULT_CONFIG = {
    "database": {
        "path": "eigenlayer_data.duckdb",
        "auto_vacuum": True,
        "timeout": 30
    },
    "cache": {
        "directory": ".cache",
        "default_ttl": 3600,
        "max_size_mb": 1000,
        "cleanup_interval": 86400
    },
    "network": {
        "rpc_url": "https://eth.llamarpc.com",
        "rpc_timeout": 30,
        "max_retries": 3,
        "rate_limit_delay": 0.1
    },
    "api": {
        "coingecko_api_key": None,
        "coingecko_base_url": "https://api.coingecko.com/api/v3",
        "coingecko_timeout": 10
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "file": None
    },
    "contracts": {
        "delegation_manager": "0x39053D51B77DC0d36036Fc1fCc8Cb819df8Ef37A",
        "strategy_manager": "0x858646372CC42E1A627fcE94aa7A7033e7CF075A",
        "allocation_manager": "0xA44151489861Fe9e3055d95adC98FbD462B948e7",
        "avs_directory": "0x135DDa560e946695d6f155dACaFC6f1F25C1F5AF"
    }
}