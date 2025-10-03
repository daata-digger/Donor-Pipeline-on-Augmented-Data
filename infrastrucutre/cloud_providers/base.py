"""
Base Cloud Provider Interface
All cloud provider implementations should inherit from this base class.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

class CloudProvider(ABC):
    @abstractmethod
    def initialize_storage(self):
        """Initialize storage (S3, Azure Blob, etc)"""
        pass

    @abstractmethod
    def initialize_compute(self):
        """Initialize compute resources (EMR, Databricks, etc)"""
        pass

    @abstractmethod
    def initialize_warehouse(self):
        """Initialize data warehouse (Redshift, Synapse, etc)"""
        pass

    @abstractmethod
    def get_storage_client(self):
        """Get client for cloud storage"""
        pass

    @abstractmethod
    def get_compute_client(self):
        """Get client for compute resources"""
        pass

    @abstractmethod
    def get_warehouse_client(self):
        """Get client for data warehouse"""
        pass

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str):
        """Upload file to cloud storage"""
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str):
        """Download file from cloud storage"""
        pass

    @abstractmethod
    def run_compute_job(self, job_config: Dict):
        """Run a compute job (Spark etc)"""
        pass

    @abstractmethod
    def execute_warehouse_query(self, query: str) -> List[Dict]:
        """Execute query in data warehouse"""
        pass