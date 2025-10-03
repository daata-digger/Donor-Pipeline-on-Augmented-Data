"""
Data quality tests
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import great_expectations as ge
from donor_analytics_enterprise.core.analytics import DonorAnalytics

def test_donor_data_quality(donor_analytics):
    """Test donor data quality"""
    donor_suite = ge.dataset.PandasDataset(donor_analytics.donors)
    
    # Test required fields
    assert donor_suite.expect_column_to_exist('donor_id')['success']
    assert donor_suite.expect_column_to_exist('email')['success']
    assert donor_suite.expect_column_to_exist('state')['success']
    
    # Test data types
    assert donor_suite.expect_column_values_to_be_of_type(
        'donor_id', 'int64'
    )['success']
    
    # Test unique constraints
    assert donor_suite.expect_column_values_to_be_unique(
        'donor_id'
    )['success']
    
    # Test email format
    assert donor_suite.expect_column_values_to_match_regex(
        'email',
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )['success']
    
def test_donation_data_quality(donor_analytics):
    """Test donation data quality"""
    donation_suite = ge.dataset.PandasDataset(donor_analytics.donations)
    
    # Test required fields
    assert donation_suite.expect_column_to_exist('donation_id')['success']
    assert donation_suite.expect_column_to_exist('donor_id')['success']
    assert donation_suite.expect_column_to_exist('amount')['success']
    
    # Test data types
    assert donation_suite.expect_column_values_to_be_of_type(
        'amount', 'float64'
    )['success']
    
    # Test value constraints
    assert donation_suite.expect_column_values_to_be_between(
        'amount', 0, None
    )['success']
    
def test_feature_engineering_quality(donor_analytics):
    """Test feature engineering quality"""
    features = donor_analytics.compute_rfm_features(
        donor_analytics.donors,
        donor_analytics.donations
    )
    
    feature_suite = ge.dataset.PandasDataset(features)
    
    # Test RFM features
    assert feature_suite.expect_column_values_to_be_between(
        'recency_days', 0, None
    )['success']
    
    assert feature_suite.expect_column_values_to_be_between(
        'frequency', 1, None
    )['success']
    
    assert feature_suite.expect_column_values_to_be_between(
        'monetary', 0, None
    )['success']
    
def test_data_completeness(donor_analytics):
    """Test data completeness"""
    # Test donor completeness
    assert donor_analytics.donors['donor_id'].notnull().all()
    assert donor_analytics.donors['email'].notnull().all()
    
    # Test donation completeness
    assert donor_analytics.donations['donation_id'].notnull().all()
    assert donor_analytics.donations['donor_id'].notnull().all()
    assert donor_analytics.donations['amount'].notnull().all()
    
def test_referential_integrity(donor_analytics):
    """Test referential integrity"""
    # Test donor_id foreign key
    donation_donor_ids = set(donor_analytics.donations['donor_id'])
    donor_ids = set(donor_analytics.donors['donor_id'])
    
    assert donation_donor_ids.issubset(donor_ids)
    
def test_temporal_consistency(donor_analytics):
    """Test temporal consistency"""
    donations = donor_analytics.donations
    
    # Test donation dates are not in future
    assert (pd.to_datetime(donations['donation_date']) <= pd.Timestamp.now()).all()
    
    # Test join dates are before first donation
    donor_first_donations = donations.groupby('donor_id')['donation_date'].min()
    donors_with_join = donor_analytics.donors[
        donor_analytics.donors['join_date'].notnull()
    ]
    
    for _, donor in donors_with_join.iterrows():
        if donor['donor_id'] in donor_first_donations:
            assert pd.to_datetime(donor['join_date']) <= pd.to_datetime(
                donor_first_donations[donor['donor_id']]
            )