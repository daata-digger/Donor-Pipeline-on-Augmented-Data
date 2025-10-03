"""
Azure Provider Implementation
"""
from azure.storage.blob import BlobServiceClient
from azure.synapse.spark import SparkClient
from azure.synapse.analytics import AnalyticsClient
from typing import Dict, List, Optional

from .base import CloudProvider

class AzureProvider(CloudProvider):
    def __init__(self, config: Dict):
        self.config = config
        self.blob_service = None
        self.spark_client = None
        self.synapse_client = None

    def initialize_storage(self):
        connection_string = self.config.get('AZURE_STORAGE_CONNECTION_STRING')
        self.blob_service = BlobServiceClient.from_connection_string(connection_string)

    def initialize_compute(self):
        workspace_url = self.config.get('AZURE_SYNAPSE_WORKSPACE_URL')
        token_credential = self.config.get('AZURE_TOKEN_CREDENTIAL')
        self.spark_client = SparkClient(workspace_url, token_credential)

    def initialize_warehouse(self):
        workspace_url = self.config.get('AZURE_SYNAPSE_WORKSPACE_URL')
        token_credential = self.config.get('AZURE_TOKEN_CREDENTIAL')
        self.synapse_client = AnalyticsClient(workspace_url, token_credential)

    def get_storage_client(self):
        return self.blob_service

    def get_compute_client(self):
        return self.spark_client

    def get_warehouse_client(self):
        return self.synapse_client

    def upload_file(self, local_path: str, remote_path: str):
        container_name = self.config.get('AZURE_CONTAINER_NAME')
        blob_client = self.blob_service.get_blob_client(
            container=container_name,
            blob=remote_path
        )
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data)

    def download_file(self, remote_path: str, local_path: str):
        container_name = self.config.get('AZURE_CONTAINER_NAME')
        blob_client = self.blob_service.get_blob_client(
            container=container_name,
            blob=remote_path
        )
        with open(local_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    def run_compute_job(self, job_config: Dict):
        return self.spark_client.create_spark_batch_job(**job_config)

    def execute_warehouse_query(self, query: str) -> List[Dict]:
        return self.synapse_client.run_query(query).result()