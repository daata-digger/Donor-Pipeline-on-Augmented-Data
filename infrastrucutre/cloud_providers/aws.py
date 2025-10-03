"""
AWS Provider Implementation
"""
import boto3
import snowflake.connector
from typing import Dict, List, Optional

from .base import CloudProvider

class AWSProvider(CloudProvider):
    def __init__(self, config: Dict):
        self.config = config
        self.s3_client = None
        self.emr_client = None
        self.snowflake_conn = None

    def initialize_storage(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=self.config.get('AWS_SECRET_ACCESS_KEY'),
            region_name=self.config.get('AWS_REGION')
        )

    def initialize_compute(self):
        self.emr_client = boto3.client(
            'emr',
            aws_access_key_id=self.config.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=self.config.get('AWS_SECRET_ACCESS_KEY'),
            region_name=self.config.get('AWS_REGION')
        )

    def initialize_warehouse(self):
        self.snowflake_conn = snowflake.connector.connect(
            user=self.config.get('SNOWFLAKE_USER'),
            password=self.config.get('SNOWFLAKE_PASSWORD'),
            account=self.config.get('SNOWFLAKE_ACCOUNT'),
            warehouse=self.config.get('SNOWFLAKE_WAREHOUSE'),
            database=self.config.get('SNOWFLAKE_DATABASE')
        )

    def get_storage_client(self):
        return self.s3_client

    def get_compute_client(self):
        return self.emr_client

    def get_warehouse_client(self):
        return self.snowflake_conn

    def upload_file(self, local_path: str, remote_path: str):
        bucket = self.config.get('AWS_BUCKET')
        self.s3_client.upload_file(local_path, bucket, remote_path)

    def download_file(self, remote_path: str, local_path: str):
        bucket = self.config.get('AWS_BUCKET')
        self.s3_client.download_file(bucket, remote_path, local_path)

    def run_compute_job(self, job_config: Dict):
        return self.emr_client.run_job_flow(**job_config)

    def execute_warehouse_query(self, query: str) -> List[Dict]:
        cur = self.snowflake_conn.cursor().execute(query)
        return cur.fetchall()