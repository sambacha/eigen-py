"""
Database schema definition and creation.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DatabaseSchema:
    """Manages database schema creation and maintenance."""
    
    def __init__(self, connection):
        """
        Initialize schema manager.
        
        Args:
            connection: DuckDB connection
        """
        self.conn = connection
    
    def create_all_tables(self) -> None:
        """Create all database tables."""
        logger.info("Creating database tables...")
        
        # Drop existing tables in dependency order
        self._drop_tables()
        
        # Create tables in dependency order
        self._create_operators_table()
        self._create_operator_metrics_table()
        self._create_avs_table()
        self._create_avs_metrics_table()
        self._create_operator_avs_registrations_table()
        self._create_strategies_table()
        self._create_operator_strategy_shares_table()
        self._create_avs_strategy_shares_table()
        
        logger.info("All tables created successfully")
    
    def _drop_tables(self) -> None:
        """Drop existing tables."""
        tables_to_drop = [
            'avs_strategy_shares', 'operator_strategy_shares',
            'operator_avs_registrations', 'operator_metrics',
            'avs_metrics', 'strategies', 'avs', 'operators'
        ]
        
        for table in tables_to_drop:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                logger.warning(f"Failed to drop table {table}: {e}")
    
    def _create_operators_table(self) -> None:
        """Create operators table."""
        self.conn.execute("""
            CREATE TABLE operators (
                operator_address VARCHAR PRIMARY KEY,
                name VARCHAR,
                website VARCHAR,
                description TEXT,
                logo VARCHAR,
                twitter VARCHAR,
                is_active BOOLEAN DEFAULT TRUE,
                earnings_receiver VARCHAR,
                delegation_approver VARCHAR,
                staker_opt_out_blocks INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_operator_metrics_table(self) -> None:
        """Create operator metrics table."""
        self.conn.execute("""
            CREATE TABLE operator_metrics (
                operator_address VARCHAR,
                timestamp TIMESTAMP,
                block_number BIGINT,
                num_stakers INTEGER DEFAULT 0,
                eth_tvl DECIMAL(30, 18) DEFAULT 0,
                eigen_tvl DECIMAL(30, 18) DEFAULT 0,
                total_tvl_usd DECIMAL(30, 2) DEFAULT 0,
                data_source VARCHAR DEFAULT 'unknown',
                PRIMARY KEY (operator_address, timestamp),
                FOREIGN KEY (operator_address) REFERENCES operators(operator_address)
            )
        """)
    
    def _create_avs_table(self) -> None:
        """Create AVS table."""
        self.conn.execute("""
            CREATE TABLE avs (
                avs_address VARCHAR PRIMARY KEY,
                name VARCHAR,
                website VARCHAR,
                description TEXT,
                logo VARCHAR,
                twitter VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_avs_metrics_table(self) -> None:
        """Create AVS metrics table."""
        self.conn.execute("""
            CREATE TABLE avs_metrics (
                avs_address VARCHAR,
                timestamp TIMESTAMP,
                operator_count INTEGER DEFAULT 0,
                staker_count INTEGER DEFAULT 0,
                eth_tvl DECIMAL(30, 18) DEFAULT 0,
                total_tvl_usd DECIMAL(30, 2) DEFAULT 0,
                PRIMARY KEY (avs_address, timestamp),
                FOREIGN KEY (avs_address) REFERENCES avs(avs_address)
            )
        """)
    
    def _create_operator_avs_registrations_table(self) -> None:
        """Create operator-AVS registrations table."""
        self.conn.execute("""
            CREATE TABLE operator_avs_registrations (
                operator_address VARCHAR,
                avs_address VARCHAR,
                registration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (operator_address, avs_address),
                FOREIGN KEY (operator_address) REFERENCES operators(operator_address),
                FOREIGN KEY (avs_address) REFERENCES avs(avs_address)
            )
        """)
    
    def _create_strategies_table(self) -> None:
        """Create strategies table."""
        self.conn.execute("""
            CREATE TABLE strategies (
                strategy_address VARCHAR PRIMARY KEY,
                symbol VARCHAR UNIQUE,
                name VARCHAR,
                underlying_token VARCHAR,
                coingecko_id VARCHAR,
                decimals INTEGER DEFAULT 18,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_operator_strategy_shares_table(self) -> None:
        """Create operator strategy shares table."""
        self.conn.execute("""
            CREATE TABLE operator_strategy_shares (
                operator_address VARCHAR,
                strategy_address VARCHAR,
                timestamp TIMESTAMP,
                block_number BIGINT,
                shares DECIMAL(38, 0) DEFAULT 0,
                tokens DECIMAL(30, 18) DEFAULT 0,
                usd_value DECIMAL(30, 2) DEFAULT 0,
                PRIMARY KEY (operator_address, strategy_address, timestamp),
                FOREIGN KEY (operator_address) REFERENCES operators(operator_address),
                FOREIGN KEY (strategy_address) REFERENCES strategies(strategy_address)
            )
        """)
    
    def _create_avs_strategy_shares_table(self) -> None:
        """Create AVS strategy shares table."""
        self.conn.execute("""
            CREATE TABLE avs_strategy_shares (
                avs_address VARCHAR,
                strategy_address VARCHAR,
                timestamp TIMESTAMP,
                shares DECIMAL(38, 0) DEFAULT 0,
                PRIMARY KEY (avs_address, strategy_address, timestamp),
                FOREIGN KEY (avs_address) REFERENCES avs(avs_address),
                FOREIGN KEY (strategy_address) REFERENCES strategies(strategy_address)
            )
        """)
    
    def create_all_indexes(self) -> None:
        """Create database indexes for performance."""
        logger.info("Creating database indexes...")
        
        indexes = [
            "CREATE INDEX idx_operator_metrics_tvl ON operator_metrics(total_tvl_usd DESC)",
            "CREATE INDEX idx_operator_metrics_time ON operator_metrics(timestamp DESC)",
            "CREATE INDEX idx_avs_metrics_time ON avs_metrics(timestamp DESC)",
            "CREATE INDEX idx_operator_avs_active ON operator_avs_registrations(is_active)",
            "CREATE INDEX idx_operator_shares_time ON operator_strategy_shares(timestamp DESC)",
            "CREATE INDEX idx_operator_shares_value ON operator_strategy_shares(usd_value DESC)",
            "CREATE INDEX idx_operators_name ON operators(name)",
            "CREATE INDEX idx_avs_name ON avs(name)"
        ]
        
        for idx in indexes:
            try:
                self.conn.execute(idx)
            except Exception as e:
                logger.warning(f"Index creation failed: {e}")
        
        logger.info("Database indexes created")
    
    def create_all_views(self) -> None:
        """Create database views for common queries."""
        logger.info("Creating database views...")
        
        self._create_operator_current_stats_view()
        self._create_avs_current_stats_view()
        self._create_top_operators_view()
        self._create_operator_avs_matrix_view()
        self._create_strategy_distribution_view()
        self._create_system_aggregate_view()
        
        logger.info("Database views created")
    
    def _create_operator_current_stats_view(self) -> None:
        """Create current operator statistics view."""
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_operator_current_stats AS
            SELECT 
                o.operator_address,
                o.name,
                o.website,
                o.twitter,
                om.num_stakers,
                om.eth_tvl,
                om.eigen_tvl,
                om.total_tvl_usd,
                om.timestamp as last_updated,
                COUNT(DISTINCT oar.avs_address) as avs_count
            FROM operators o
            LEFT JOIN operator_metrics om ON o.operator_address = om.operator_address
            LEFT JOIN operator_avs_registrations oar ON o.operator_address = oar.operator_address 
                AND oar.is_active = TRUE
            WHERE om.timestamp = (
                SELECT MAX(timestamp) 
                FROM operator_metrics om2 
                WHERE om2.operator_address = o.operator_address
            )
            GROUP BY o.operator_address, o.name, o.website, o.twitter, 
                     om.num_stakers, om.eth_tvl, om.eigen_tvl, om.total_tvl_usd, om.timestamp
        """)
    
    def _create_avs_current_stats_view(self) -> None:
        """Create current AVS statistics view."""
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_avs_current_stats AS
            SELECT 
                a.avs_address,
                a.name,
                a.website,
                am.operator_count,
                am.staker_count,
                am.eth_tvl,
                am.total_tvl_usd,
                am.timestamp as last_updated
            FROM avs a
            LEFT JOIN avs_metrics am ON a.avs_address = am.avs_address
            WHERE am.timestamp = (
                SELECT MAX(timestamp) 
                FROM avs_metrics am2 
                WHERE am2.avs_address = a.avs_address
            )
        """)
    
    def _create_top_operators_view(self) -> None:
        """Create top operators view."""
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_top_operators_by_tvl AS
            SELECT 
                operator_address,
                name,
                total_tvl_usd,
                eth_tvl,
                eigen_tvl,
                num_stakers,
                avs_count,
                RANK() OVER (ORDER BY total_tvl_usd DESC) as tvl_rank
            FROM v_operator_current_stats
            ORDER BY total_tvl_usd DESC
        """)
    
    def _create_operator_avs_matrix_view(self) -> None:
        """Create operator-AVS matrix view."""
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_operator_avs_matrix AS
            SELECT 
                o.name as operator_name,
                o.operator_address,
                a.name as avs_name,
                a.avs_address,
                oar.is_active,
                oar.registration_timestamp
            FROM operator_avs_registrations oar
            JOIN operators o ON oar.operator_address = o.operator_address
            JOIN avs a ON oar.avs_address = a.avs_address
            WHERE oar.is_active = TRUE
            ORDER BY o.name, a.name
        """)
    
    def _create_strategy_distribution_view(self) -> None:
        """Create strategy distribution view."""
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_strategy_distribution AS
            SELECT 
                s.symbol,
                s.name as strategy_name,
                COUNT(DISTINCT oss.operator_address) as operator_count,
                SUM(oss.tokens) as total_tokens,
                SUM(oss.usd_value) as total_usd_value,
                AVG(oss.usd_value) as avg_usd_value_per_operator
            FROM operator_strategy_shares oss
            JOIN strategies s ON oss.strategy_address = s.strategy_address
            WHERE oss.timestamp = (
                SELECT MAX(timestamp) 
                FROM operator_strategy_shares oss2 
                WHERE oss2.operator_address = oss.operator_address 
                AND oss2.strategy_address = oss.strategy_address
            )
            GROUP BY s.symbol, s.name
            ORDER BY total_usd_value DESC
        """)
    
    def _create_system_aggregate_view(self) -> None:
        """Create system aggregate metrics view."""
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_system_aggregate_metrics AS
            SELECT 
                COUNT(DISTINCT o.operator_address) as total_operators,
                COUNT(DISTINCT a.avs_address) as total_avs,
                COUNT(DISTINCT oar.operator_address || '-' || oar.avs_address) as total_registrations,
                COALESCE(SUM(om.total_tvl_usd), 0) as total_system_tvl_usd,
                COALESCE(SUM(om.eth_tvl), 0) as total_system_eth_tvl,
                COALESCE(SUM(om.eigen_tvl), 0) as total_system_eigen_tvl,
                COALESCE(SUM(om.num_stakers), 0) as total_unique_stakers,
                COALESCE(AVG(om.total_tvl_usd), 0) as avg_operator_tvl_usd,
                MAX(om.timestamp) as last_update
            FROM operators o
            LEFT JOIN operator_metrics om ON o.operator_address = om.operator_address
            LEFT JOIN operator_avs_registrations oar ON o.operator_address = oar.operator_address
            LEFT JOIN avs a ON oar.avs_address = a.avs_address
            WHERE om.timestamp = (
                SELECT MAX(timestamp) 
                FROM operator_metrics om2 
                WHERE om2.operator_address = o.operator_address
            )
        """)
    
    def import_strategies(self) -> None:
        """Import strategy reference data."""
        strategies = [
            ("0x93c4b944D05dfe6df7645A86cd2206016c51564D", "stETH", "Lido Staked ETH", 
             "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", "staked-ether", 18),
            ("0x1BeE69b7dFFfA4E2d53C2a2Df135C388AD25dCD2", "rETH", "Rocket Pool ETH", 
             "0xae78736Cd615f374D3085123A210448E74Fc6393", "rocket-pool-eth", 18),
            ("0x54945180dB7943c0ed0FEE7EdaB2Bd24620256bc", "cbETH", "Coinbase Wrapped ETH", 
             "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704", "coinbase-wrapped-staked-eth", 18),
            ("0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7", "EIGEN", "Eigen", 
             "0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83", "eigenlayer", 18),
            ("0xbeaC0eeEeeeeEEeEeEEEEeeEEeEeeeEeeEEBEaC0", "beaconETH", "Beacon ETH", 
             "0xbeaC0eeEeeeeEEeEeEEEEeeEEeEeeeEeeEEBEaC0", "ethereum", 18)
        ]
        
        for strategy in strategies:
            self.conn.execute("""
                INSERT INTO strategies 
                (strategy_address, symbol, name, underlying_token, coingecko_id, decimals)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (strategy_address) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    underlying_token = EXCLUDED.underlying_token,
                    coingecko_id = EXCLUDED.coingecko_id,
                    decimals = EXCLUDED.decimals
            """, strategy)
        
        self.conn.commit()
        logger.info("Strategy data imported")