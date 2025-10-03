"""
Main Terraform configuration for donor analytics infrastructure
"""

terraform {
  required_version = ">= 1.4.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 0.74.0"
    }
  }
  
  backend "s3" {
    bucket = "ujadonor-terraform-state"
    key    = "donor-analytics/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "donor-analytics"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  role     = var.snowflake_role
}

# S3 Data Lake
module "data_lake" {
  source = "./modules/data_lake"
  
  project_prefix = var.project_prefix
  environment    = var.environment
  
  raw_bucket_name       = "${var.project_prefix}-raw-${var.environment}"
  processed_bucket_name = "${var.project_prefix}-processed-${var.environment}"
  ml_bucket_name       = "${var.project_prefix}-ml-${var.environment}"
}

# Snowflake Resources
module "snowflake" {
  source = "./modules/snowflake"
  
  database_name    = upper("${var.project_prefix}_${var.environment}")
  warehouse_name   = upper("${var.project_prefix}_WH_${var.environment}")
  warehouse_size   = var.snowflake_warehouse_size
  raw_schema      = "RAW"
  curated_schema  = "CURATED"
  analytics_schema = "ANALYTICS"
}

# EMR Cluster
module "spark_cluster" {
  source = "./modules/emr"
  
  cluster_name           = "${var.project_prefix}-emr-${var.environment}"
  release_label         = "emr-6.5.0"
  master_instance_type  = var.emr_master_instance_type
  core_instance_type    = var.emr_core_instance_type
  core_instance_count   = var.emr_core_instance_count
  bootstrap_script_path = "s3://${module.data_lake.ml_bucket}/bootstrap/install_requirements.sh"
}

# MLflow Tracking
module "mlflow" {
  source = "./modules/mlflow"
  
  app_name     = "${var.project_prefix}-mlflow-${var.environment}"
  environment  = var.environment
  instance_type = "t3.small"
  min_size     = 1
  max_size     = 1
}

# Airflow Resources
module "airflow" {
  source = "./modules/airflow"
  
  project_prefix = var.project_prefix
  environment    = var.environment
  
  airflow_conn_secret  = var.airflow_sql_alchemy_conn
  webserver_secret     = var.airflow_webserver_secret
  log_retention_days   = 30
}

# IAM Roles and Policies
module "iam" {
  source = "./modules/iam"
  
  project_prefix = var.project_prefix
  environment    = var.environment
  
  s3_bucket_arns = [
    module.data_lake.raw_bucket_arn,
    module.data_lake.processed_bucket_arn,
    module.data_lake.ml_bucket_arn
  ]
}

# Monitoring & Alerting
module "monitoring" {
  source = "./modules/monitoring"
  
  project_prefix = var.project_prefix
  environment    = var.environment
  
  alert_email    = var.alert_email
  slack_webhook  = var.slack_webhook
  log_retention  = 30
}
