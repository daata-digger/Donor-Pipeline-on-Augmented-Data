"""
Enterprise-grade PySpark ETL pipeline for donor data
Features:
- Cloud storage integration (S3/Azure)
- Data quality validation
- Schema enforcement
- Incremental processing
- Error handling and logging
- Performance optimization
"""
import os
import argparse
import yaml
from datetime import datetime
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as _sum, count as _count, lit, 
    current_timestamp, year, month, dayofmonth
)
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, max as _max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Load configurations
def load_config() -> Dict:
    config_path = os.environ.get('CONFIG_PATH', 'configs/etl_config.yml')
    with open(config_path) as f:
        return yaml.safe_load(f)

config = load_config()

# Initialize Spark with cloud storage connectors
spark = (SparkSession.builder
         .appName("DonorAnalyticsETL")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.streaming.metricsEnabled", "true")
         .getOrCreate())

# Set up logging
log4j = spark._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)

# Storage paths from config
RAW_BASE = config['storage']['raw_path']
CURATED_BASE = config['storage']['curated_path']
ARCHIVE_BASE = config['storage']['archive_path']

# Schema definitions
schemas = {
    'donors': StructType([
        StructField('donor_id', StringType(), False),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('postal_code', StringType(), True),
        StructField('country', StringType(), True),
        StructField('birth_year', StringType(), True),
        StructField('join_date', DateType(), True),
        StructField('wealth_index', DoubleType(), True),
        StructField('engagement_index', DoubleType(), True)
    ]),
    'donations': StructType([
        StructField('donation_id', StringType(), False),
        StructField('donor_id', StringType(), False),
        StructField('campaign_id', StringType(), True),
        StructField('program', StringType(), True),
        StructField('amount', DoubleType(), True),
        StructField('donation_date', DateType(), True)
    ])
}

def read_source(source_name: str, partition_date: str = None) -> None:
    """Read source data with schema validation and partitioning"""
    logger.info(f"Reading {source_name} data...")
    
    # Build source path
    source_path = f"{RAW_BASE}/{source_name}"
    if partition_date:
        source_path = f"{source_path}/dt={partition_date}"
    
    # Read with schema validation
    df = (spark.read
          .option("header", True)
          .schema(schemas.get(source_name))
          .csv(source_path))
    
    # Log metrics
    logger.info(f"Read {df.count()} rows from {source_name}")
    return df

def validate_data_quality(df, rules: List[Dict]) -> bool:
    """Apply data quality rules"""
    for rule in rules:
        if rule['type'] == 'not_null':
            null_count = df.filter(col(rule['column']).isNull()).count()
            if null_count > rule['threshold']:
                logger.error(f"Data quality check failed: {rule['column']} has {null_count} null values")
                return False
    return True

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--source', help='Source to process')
parser.add_argument('--date', help='Processing date (YYYY-MM-DD)')
args = parser.parse_args()

# Process date for partitioning
process_date = args.date or datetime.now().strftime('%Y-%m-%d')

try:
    # Read source data
    donors = read_source('donors', process_date)
    donations = read_source('donations', process_date)
    events = read_source('events', process_date)
    wealth = read_source('wealth_external', process_date)

    # Transform donations
    donations = donations.withColumn("donation_date", to_date(col("donation_date"))) \
                       .withColumn("amount", col("amount").cast("double"))

    # Create fact and dimension tables
    fact_donation = donations.select(
        "donation_id", "donor_id", "campaign_id", "program", "amount", "donation_date"
    )
    
    dim_donor = donors.select(
        "donor_id", "first_name", "last_name", "email", "city", "state", 
        "postal_code", "country", "birth_year", "join_date", 
        "wealth_index", "engagement_index"
    )

    # Calculate RFM metrics
    rfm = fact_donation.groupBy("donor_id").agg(
        _sum("amount").alias("total_amount"),
        _count("*").alias("frequency"),
        _max("donation_date").alias("last_gift")
    ).withColumn("recency_days", datediff(current_timestamp(), col("last_gift")))

    # Join features
    features = (dim_donor.join(rfm, "donor_id", "left")
               .join(events, "donor_id", "left")
               .join(wealth, "donor_id", "left")
               .fillna({
                   "total_amount": 0,
                   "frequency": 0,
                   "recency_days": 9999,
                   "events_attended": 0,
                   "volunteer_hours": 0,
                   "wealth_score_ext": 0
               }))

    # Validate data quality
    quality_rules = config['data_quality_rules']
    if not all(validate_data_quality(df, quality_rules) for df in [features, fact_donation, dim_donor]):
        raise Exception("Data quality validation failed")

    # Write to curated layer with partitioning
    partition_cols = ['year', 'month', 'day']
    for df, name in [(features, 'donor_features'),
                     (fact_donation, 'fact_donation'),
                     (dim_donor, 'dim_donor')]:
        
        # Add partition columns
        df = (df.withColumn('year', year(current_timestamp()))
              .withColumn('month', month(current_timestamp()))
              .withColumn('day', dayofmonth(current_timestamp())))
        
        # Write partitioned Parquet
        (df.write
         .partitionBy(partition_cols)
         .mode("overwrite")
         .parquet(f"{CURATED_BASE}/{name}"))
        
        logger.info(f"Successfully wrote {name} to {CURATED_BASE}")

except Exception as e:
    logger.error(f"ETL job failed: {str(e)}")
    raise e

finally:
    spark.stop()
