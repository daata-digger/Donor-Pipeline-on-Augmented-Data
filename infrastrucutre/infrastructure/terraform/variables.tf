"""
Variables for donor analytics infrastructure
"""

# General
variable "environment" {
  description = "Deployment environment (dev/staging/prod)"
  type        = string
  default     = "prod"
}

variable "project_prefix" {
  description = "Prefix for all resources"
  type        = string
  default     = "donor-analytics"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

# Snowflake
variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake admin username"
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake role to use"
  type        = string
  default     = "SYSADMIN"
}

variable "snowflake_warehouse_size" {
  description = "Size of the Snowflake warehouse"
  type        = string
  default     = "X-SMALL"
}

# EMR Configuration
variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "m5.2xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 2
}

# Airflow
variable "airflow_sql_alchemy_conn" {
  description = "Airflow SQL Alchemy connection string"
  type        = string
  sensitive   = true
}

variable "airflow_webserver_secret" {
  description = "Secret key for Airflow webserver"
  type        = string
  sensitive   = true
}

# Monitoring
variable "alert_email" {
  description = "Email address for alerts"
  type        = string
  default     = "data-alerts@ujafed.org"
}

variable "slack_webhook" {
  description = "Slack webhook URL for alerts"
  type        = string
  sensitive   = true
}

# VPC Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24"]
}
