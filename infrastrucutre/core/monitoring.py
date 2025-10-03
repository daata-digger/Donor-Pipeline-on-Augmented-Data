"""
Monitoring utilities for the data pipeline
"""
import pandas as pd
import json
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Any
import pathlib

# Optional imports for different monitoring capabilities
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

try:
    from airflow.models import DagRun
    from airflow.utils.db import create_session
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

class PipelineMonitor:
    def __init__(self):
        self.root = pathlib.Path(__file__).resolve().parents[2]
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load monitoring configuration"""
        try:
            with open(self.root / 'config/monitoring.json') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def get_ingestion_metrics(self) -> Dict[str, Dict[str, str]]:
        """Get real-time metrics from data sources"""
        metrics = {}
        
        # Salesforce NPSP metrics
        try:
            sf_stats = self._get_salesforce_metrics()
            metrics['salesforce_npsp'] = {
                'status': 'Healthy' if sf_stats['sync_status'] == 'Success' else 'Warning',
                'last_sync': sf_stats['last_sync_time'],
                'records': f"{sf_stats['record_count']:,} donors",
                'latency': f"{sf_stats['latency_minutes']} mins"
            }
        except Exception as e:
            metrics['salesforce_npsp'] = {
                'status': 'Error',
                'last_sync': 'Unknown',
                'records': 'Error',
                'latency': 'Error'
            }
            
        # Similar implementations for other sources
        return metrics

    def get_pipeline_status(self) -> Dict[str, Dict[str, str]]:
        """Get status of Airflow DAGs and dbt jobs"""
        status = {}
        
        # Get Airflow metrics
        with create_session() as session:
            runs = session.query(DagRun).filter(
                DagRun.execution_date > datetime.now() - timedelta(days=1)
            ).all()
            
            status['ingestion_dag'] = {
                'status': runs[-1].state if runs else 'Unknown',
                'runtime': f"{runs[-1].end_date - runs[-1].start_date}",
                'last_run': runs[-1].execution_date.strftime('%Y-%m-%d %H:%M'),
                'success_rate': f"{sum(1 for r in runs if r.state=='success')/len(runs)*100:.1f}%"
            }
        
        # Get dbt metrics
        try:
            dbt_results = self._get_dbt_results()
            status['dbt_transformations'] = {
                'status': 'Success' if dbt_results['failed_tests'] == 0 else 'Warning',
                'runtime': f"{dbt_results['runtime_minutes']} mins",
                'models': f"{dbt_results['passed_models']}/{dbt_results['total_models']} passed",
                'test_coverage': f"{dbt_results['test_coverage']}%"
            }
        except Exception as e:
            status['dbt_transformations'] = {
                'status': 'Error',
                'runtime': 'Unknown',
                'models': 'Error',
                'test_coverage': 'Unknown'
            }
            
        return status

    def get_warehouse_metrics(self) -> Dict[str, Any]:
        """Get Snowflake warehouse metrics"""
        try:
            with snowflake.connector.connect(**self.config['snowflake']) as conn:
                cs = conn.cursor()
                
                # Get credit usage
                cs.execute("SELECT SUM(credits_used) FROM table(information_schema.warehouse_metering_history(dateadd('hours', -24, current_timestamp())))")
                credits = cs.fetchone()[0]
                
                # Get storage usage
                cs.execute("SELECT storage_bytes FROM table(information_schema.database_storage_usage_history(dateadd('hours', -24, current_timestamp())))")
                storage = cs.fetchone()[0] / (1024**4)  # Convert to TB
                
                # Get query performance
                cs.execute("""
                    SELECT 
                        COUNT(*) as total_queries,
                        COUNT(CASE WHEN execution_time < 10000 THEN 1 END) as fast_queries
                    FROM table(information_schema.query_history(dateadd('hours', -1, current_timestamp())))
                """)
                total, fast = cs.fetchone()
                
                return {
                    'credits_used': round(credits, 1),
                    'storage_tb': round(storage, 2),
                    'active_queries': self._get_active_queries(),
                    'query_performance': f"{round(fast/total*100)}% under 10s"
                }
        except Exception as e:
            return {
                'credits_used': 0,
                'storage_tb': 0,
                'active_queries': 0,
                'query_performance': 'Error'
            }

    def get_ml_metrics(self) -> Dict[str, Dict[str, str]]:
        """Get ML pipeline metrics"""
        try:
            # Get model metrics from MLflow
            model_metrics = self._get_mlflow_metrics()
            
            return {
                'model_health': {
                    'status': 'Healthy' if model_metrics['drift_score'] < 0.1 else 'Warning',
                    'drift': 'No drift detected' if model_metrics['drift_score'] < 0.1 else f"Drift score: {model_metrics['drift_score']:.2f}",
                    'accuracy': f"{model_metrics['accuracy']:.2f}",
                    'last_retrain': model_metrics['last_training_date']
                },
                'predictions': {
                    'volume': f"{model_metrics['predictions_per_day']:,} donors/day",
                    'latency': f"{model_metrics['avg_latency_ms']}ms avg",
                    'success_rate': f"{model_metrics['success_rate']*100:.1f}%"
                }
            }
        except Exception as e:
            return {
                'model_health': {
                    'status': 'Error',
                    'drift': 'Unknown',
                    'accuracy': 'Unknown',
                    'last_retrain': 'Unknown'
                },
                'predictions': {
                    'volume': 'Error',
                    'latency': 'Unknown',
                    'success_rate': 'Unknown'
                }
            }

    def _get_salesforce_metrics(self) -> Dict[str, Any]:
        """Get metrics from Salesforce NPSP"""
        # Implement Salesforce API call
        pass

    def _get_dbt_results(self) -> Dict[str, Any]:
        """Get dbt run results"""
        try:
            with open(self.root / 'logs/dbt_latest_run.json') as f:
                results = json.load(f)
            return {
                'failed_tests': sum(1 for r in results['results'] if r['status'] == 'fail'),
                'runtime_minutes': results['elapsed_time'] / 60,
                'passed_models': sum(1 for r in results['results'] if r['status'] == 'pass'),
                'total_models': len(results['results']),
                'test_coverage': results['test_coverage']
            }
        except FileNotFoundError:
            return {
                'failed_tests': 0,
                'runtime_minutes': 0,
                'passed_models': 0,
                'total_models': 0,
                'test_coverage': 0
            }

    def _get_active_queries(self) -> int:
        """Get count of active Snowflake queries"""
        try:
            with snowflake.connector.connect(**self.config['snowflake']) as conn:
                cs = conn.cursor()
                cs.execute("SELECT COUNT(*) FROM table(information_schema.query_history(result_limit => 1000)) WHERE execution_status = 'RUNNING'")
                return cs.fetchone()[0]
        except:
            return 0

    def _get_mlflow_metrics(self) -> Dict[str, Any]:
        """Get metrics from MLflow"""
        # Implement MLflow API call
        pass