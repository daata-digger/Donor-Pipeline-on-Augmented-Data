"""
Main entry point for the donor analytics package
"""
import click
from pathlib import Path
from typing import Optional

from donor_analytics_enterprise.core.analytics import DonorAnalytics
from donor_analytics_enterprise.core.visualization import DonorVisualization
from donor_analytics_enterprise.cloud_providers.aws import AWSProvider
from donor_analytics_enterprise.cloud_providers.azure import AzureProvider

@click.group()
def cli():
    """Donor Analytics Enterprise CLI"""
    pass

@cli.command()
@click.option('--cloud-provider', type=click.Choice(['aws', 'azure', 'none']), default='none')
@click.option('--config-file', type=click.Path(exists=True), help='Path to cloud config file')
@click.option('--donors-path', required=True, help='Path to donors.csv')
@click.option('--donations-path', required=True, help='Path to donations.csv')
@click.option('--campaigns-path', required=True, help='Path to campaigns.csv')
@click.option('--events-path', help='Path to engagement_events.csv')
@click.option('--wealth-path', help='Path to wealth_external.csv')
def process_data(cloud_provider, config_file, **data_paths):
    """Process donor data and generate features"""
    
    # Initialize cloud provider if specified
    provider = None
    if cloud_provider != 'none':
        if cloud_provider == 'aws':
            provider = AWSProvider(config_file)
        elif cloud_provider == 'azure':
            provider = AzureProvider(config_file)
    
    # Initialize analytics
    analytics = DonorAnalytics(cloud_provider=provider)
    
    # Run pipeline
    results = analytics.process_full_pipeline(data_paths)
    
    click.echo(f"Processed {len(results['donor_features'])} donors")
    click.echo("Key metrics:")
    for metric, value in results['metrics'].items():
        click.echo(f"  {metric}: {value}")

@cli.command()
@click.option('--data-dir', type=click.Path(exists=True), help='Path to data directory')
def run_dashboard(data_dir):
    """Launch the Streamlit dashboard"""
    viz = DonorVisualization(Path(data_dir) if data_dir else None)
    viz.run_streamlit_dashboard()

if __name__ == '__main__':
    cli()