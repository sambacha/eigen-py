"""
Main CLI entry point.
"""

import click
import sys
from pathlib import Path

from ..config.settings import Settings
from ..core.client import EigenLayerClient
from ..core.exceptions import EigenLayerError
from .commands import setup_commands


@click.group()
@click.option(
    "--config", 
    "-c", 
    type=click.Path(exists=True),
    help="Configuration file path"
)
@click.option(
    "--database", 
    "-d", 
    type=click.Path(),
    help="Database file path"
)
@click.option(
    "--cache-dir", 
    type=click.Path(),
    help="Cache directory path"
)
@click.option(
    "--log-level", 
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO",
    help="Logging level"
)
@click.option(
    "--verbose", 
    "-v", 
    is_flag=True, 
    help="Enable verbose output"
)
@click.pass_context
def cli(ctx, config, database, cache_dir, log_level, verbose):
    """
    EigenLayer Analysis CLI
    
    A comprehensive toolkit for analyzing EigenLayer operators, AVS, and strategies.
    """
    # Ensure ctx.obj exists
    ctx.ensure_object(dict)
    
    try:
        # Load settings
        settings = Settings.load(config)
        
        # Override with CLI arguments
        if database:
            settings.database.path = database
        if cache_dir:
            settings.cache.directory = cache_dir
        if verbose:
            settings.logging.level = "DEBUG"
        else:
            settings.logging.level = log_level
        
        # Setup logging
        settings.logging.setup_logging()
        
        # Validate configuration
        settings.validate()
        
        # Store in context
        ctx.obj["settings"] = settings
        
    except Exception as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize database and setup system."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            click.echo("🚀 Initializing EigenLayer analysis system...")
            
            # Setup database
            click.echo("📊 Setting up database...")
            client.setup_database()
            
            # Import initial data
            click.echo("📥 Importing data...")
            client.import_data()
            
            click.echo("✅ System initialized successfully!")
            
            # Show summary
            stats = client.db_manager.get_database_stats()
            click.echo("\n📈 Database Summary:")
            for table, count in stats.items():
                click.echo(f"  {table}: {count:,} records")
            
    except EigenLayerError as e:
        click.echo(f"❌ Initialization failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--limit", "-l", type=int, default=10, help="Number of operators to show")
@click.option("--format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def operators(ctx, limit, format):
    """List top operators by TVL."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            operators_data = client.get_top_operators(limit)
            
            if format == "json":
                import json
                click.echo(json.dumps([op.__dict__ for op in operators_data], indent=2, default=str))
            else:
                _display_operators_table(operators_data)
                
    except EigenLayerError as e:
        click.echo(f"❌ Failed to fetch operators: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def avs(ctx, format):
    """List all AVS."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            avs_data = client.get_all_avs()
            
            if format == "json":
                import json
                click.echo(json.dumps([avs.__dict__ for avs in avs_data], indent=2, default=str))
            else:
                _display_avs_table(avs_data)
                
    except EigenLayerError as e:
        click.echo(f"❌ Failed to fetch AVS: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def system(ctx):
    """Show system metrics."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            metrics = client.get_system_metrics()
            
            click.echo("📊 EigenLayer System Metrics")
            click.echo("=" * 40)
            click.echo(f"Total Operators: {metrics.total_operators:,}")
            click.echo(f"Total AVS: {metrics.total_avs:,}")
            click.echo(f"Total Registrations: {metrics.total_registrations:,}")
            click.echo(f"Total TVL (USD): ${metrics.total_system_tvl_usd:,.2f}")
            click.echo(f"Total ETH TVL: {metrics.total_system_eth_tvl:,.4f}")
            click.echo(f"Total EIGEN TVL: {metrics.total_system_eigen_tvl:,.0f}")
            click.echo(f"Total Stakers: {metrics.total_unique_stakers:,}")
            click.echo(f"Avg Operator TVL: ${metrics.avg_operator_tvl_usd:,.2f}")
            
    except EigenLayerError as e:
        click.echo(f"❌ Failed to fetch system metrics: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def cache(ctx):
    """Show cache statistics."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            stats = client.get_cache_stats()
            
            click.echo("💾 Cache Statistics")
            click.echo("=" * 30)
            
            total_files = 0
            total_size = 0
            
            for cache_type, stat in stats.items():
                click.echo(f"{cache_type:>12}: {stat.file_count:>4} files, {stat.total_size_mb:>6.2f} MB")
                total_files += stat.file_count
                total_size += stat.total_size_mb
            
            click.echo("-" * 30)
            click.echo(f"{'Total':>12}: {total_files:>4} files, {total_size:>6.2f} MB")
            
    except EigenLayerError as e:
        click.echo(f"❌ Failed to fetch cache stats: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--type", type=click.Choice(["rpc", "prices", "contracts", "all"]), default="all")
@click.confirmation_option(prompt="Are you sure you want to clear the cache?")
@click.pass_context
def clear_cache(ctx, type):
    """Clear cache files."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            cache_type = None if type == "all" else type
            client.clear_cache(cache_type)
            click.echo(f"✅ Cache cleared: {type}")
            
    except EigenLayerError as e:
        click.echo(f"❌ Failed to clear cache: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("output_file", type=click.Path())
@click.option("--format", type=click.Choice(["json", "csv"]), default="json")
@click.pass_context
def export(ctx, output_file, format):
    """Export system data to file."""
    settings = ctx.obj["settings"]
    
    try:
        with EigenLayerClient(settings=settings) as client:
            client.export_system_report(output_file)
            click.echo(f"✅ System data exported to {output_file}")
            
    except EigenLayerError as e:
        click.echo(f"❌ Export failed: {e}", err=True)
        sys.exit(1)


def _display_operators_table(operators):
    """Display operators in table format."""
    if not operators:
        click.echo("No operators found")
        return
    
    click.echo("🏆 Top Operators by TVL")
    click.echo("-" * 80)
    click.echo(f"{'Rank':<6} {'Name':<30} {'TVL (USD)':<15} {'ETH TVL':<12} {'AVS':<6}")
    click.echo("-" * 80)
    
    for i, op in enumerate(operators, 1):
        name = (op.name or "Unknown")[:28]
        click.echo(f"{i:<6} {name:<30} ${op.total_tvl_usd/1e6:>11.1f}M "
                  f"{op.eth_tvl:>10,.0f} {op.avs_count:>4}")


def _display_avs_table(avs_list):
    """Display AVS in table format."""
    if not avs_list:
        click.echo("No AVS found")
        return
    
    click.echo("🌐 Actively Validated Services")
    click.echo("-" * 80)
    click.echo(f"{'Name':<40} {'Operators':<12} {'Stakers':<12} {'TVL (USD)':<15}")
    click.echo("-" * 80)
    
    for avs in avs_list:
        name = (avs.name or "Unknown")[:38]
        click.echo(f"{name:<40} {avs.operator_count:>10} "
                  f"{avs.staker_count:>10,} ${avs.total_tvl_usd/1e6:>11.1f}M")


def main():
    """Main CLI entry point."""
    # Add commands
    setup_commands(cli)
    
    # Run CLI
    cli()


if __name__ == "__main__":
    main()