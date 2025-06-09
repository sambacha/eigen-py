"""
Data models for EigenLayer analysis.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime


@dataclass
class OperatorStats:
    """Data model for operator statistics."""
    address: str
    name: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    logo: Optional[str] = None
    twitter: Optional[str] = None
    
    # Operational data
    is_operator: bool = False
    earnings_receiver: Optional[str] = None
    delegation_approver: Optional[str] = None
    staker_opt_out_blocks: Optional[int] = None
    
    # Metrics
    num_stakers: int = 0
    eth_tvl: Decimal = field(default_factory=lambda: Decimal(0))
    eigen_tvl: Decimal = field(default_factory=lambda: Decimal(0))
    total_tvl_usd: Decimal = field(default_factory=lambda: Decimal(0))
    
    # Strategy data
    strategy_shares: Dict[str, int] = field(default_factory=dict)
    strategy_tokens: Dict[str, Decimal] = field(default_factory=dict)
    strategy_usd_values: Dict[str, Decimal] = field(default_factory=dict)
    
    # AVS data
    avs_registrations: List[str] = field(default_factory=list)
    avs_count: int = 0
    
    # Metadata
    block_number: Optional[int] = None
    timestamp: Optional[datetime] = None
    last_updated: Optional[datetime] = None


@dataclass
class AVSMetrics:
    """Data model for AVS metrics."""
    address: str
    name: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    logo: Optional[str] = None
    twitter: Optional[str] = None
    
    # Metrics
    operator_count: int = 0
    staker_count: int = 0
    eth_tvl: Decimal = field(default_factory=lambda: Decimal(0))
    total_tvl_usd: Decimal = field(default_factory=lambda: Decimal(0))
    
    # Strategy data
    strategy_shares: Dict[str, Decimal] = field(default_factory=dict)
    
    # Metadata
    timestamp: Optional[datetime] = None
    last_updated: Optional[datetime] = None


@dataclass
class StrategyInfo:
    """Data model for strategy information."""
    address: str
    symbol: str
    name: str
    underlying_token: str
    coingecko_id: str
    decimals: int = 18
    
    # Metrics
    operator_count: int = 0
    total_tokens: Decimal = field(default_factory=lambda: Decimal(0))
    total_usd_value: Decimal = field(default_factory=lambda: Decimal(0))
    avg_usd_value_per_operator: Decimal = field(default_factory=lambda: Decimal(0))
    
    # Metadata
    created_at: Optional[datetime] = None


@dataclass
class SystemMetrics:
    """Data model for system-wide metrics."""
    total_operators: int = 0
    total_avs: int = 0
    total_registrations: int = 0
    total_system_tvl_usd: Decimal = field(default_factory=lambda: Decimal(0))
    total_system_eth_tvl: Decimal = field(default_factory=lambda: Decimal(0))
    total_system_eigen_tvl: Decimal = field(default_factory=lambda: Decimal(0))
    total_unique_stakers: int = 0
    avg_operator_tvl_usd: Decimal = field(default_factory=lambda: Decimal(0))
    
    # Metadata
    last_update: Optional[datetime] = None
    analysis_timestamp: Optional[datetime] = None


@dataclass
class CacheStats:
    """Data model for cache statistics."""
    cache_type: str
    file_count: int = 0
    total_size_bytes: int = 0
    total_size_mb: float = 0.0
    hit_rate: Optional[float] = None
    avg_response_time_ms: Optional[float] = None


@dataclass
class PerformanceMetrics:
    """Data model for performance metrics."""
    operation: str
    cached_time_ms: float
    uncached_time_ms: float
    speedup_factor: float
    cache_hit: bool
    timestamp: datetime = field(default_factory=datetime.now)