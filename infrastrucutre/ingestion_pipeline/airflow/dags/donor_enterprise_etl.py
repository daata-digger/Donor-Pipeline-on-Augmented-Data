"""
Donor Analytics Enterprise ETL DAG
Orchestrates end-to-end data pipeline including:
- Data ingestion from multiple sources
- PySpark transformations for scalability
- Entity resolution for donor deduplication
- dbt transformations for analytics
- Data quality validation
- ML model training
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'donor_analytics_team',
    'depends_on_past': False,
    'email': ['data-alerts@ujafed.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'donor_enterprise_etl',
    default_args=default_args,
    description='Enterprise ETL pipeline for donor analytics',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['donor_analytics', 'enterprise', 'etl'],
) as dag:
    
    # Task group for data ingestion
    with TaskGroup('data_ingestion') as ingest_group:
        ingest_campaigns = SparkSubmitOperator(
            task_id='ingest_campaigns',
            application='{{var.value.project_root}}/ingestion_pyspark/spark_ingest_transform.py',
            conf={'spark.driver.memory': '4g'},
            application_args=['--source', 'campaigns']
        )
        
        ingest_donations = SparkSubmitOperator(
            task_id='ingest_donations',
            application='{{var.value.project_root}}/ingestion_pyspark/spark_ingest_transform.py',
            conf={'spark.driver.memory': '4g'},
            application_args=['--source', 'donations']
        )
        
        ingest_donors = SparkSubmitOperator(
            task_id='ingest_donors',
            application='{{var.value.project_root}}/ingestion_pyspark/spark_ingest_transform.py',
            conf={'spark.driver.memory': '4g'},
            application_args=['--source', 'donors']
        )
    
    # Entity resolution
    entity_resolution = SparkSubmitOperator(
        task_id='entity_resolution',
        application='{{var.value.project_root}}/ingestion_pyspark/spark_entity_resolution.py',
        conf={'spark.driver.memory': '8g'}
    )
    
    # dbt transformations
    dbt_transform = DbtCloudRunJobOperator(
        task_id='dbt_transform',
        dbt_cloud_conn_id='dbt_cloud',
        job_id='{{var.value.dbt_job_id}}',
        check_interval=60,
        timeout=3600
    )
    
    # Data quality checks
    run_dq_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable='run_quality_checks',
        op_kwargs={
            'tables': ['donors', 'donations', 'campaigns'],
            'quality_config': '{{var.value.project_root}}/configs/data_quality.yml'
        }
    )
    
    # ML model training with MLflow tracking
    train_models = PythonOperator(
        task_id='train_ml_models',
        python_callable='train_models',
        op_kwargs={
            'model_config': '{{var.value.project_root}}/configs/model_config.yml',
            'experiment_name': 'donor_propensity',
            'mlflow_tracking_uri': Variable.get('mlflow_tracking_uri')
        }
    )
    
    # Define dependencies
    ingest_group >> entity_resolution >> dbt_transform >> run_dq_checks >> train_models
