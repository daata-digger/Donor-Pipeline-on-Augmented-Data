"""
Test suite for advanced ML models
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from donor_analytics_enterprise.ml.advanced_models import (
    DonorLifetimeValue,
    DonorSegmentation,
    DonorChurnPrediction
)

@pytest.fixture
def sample_data():
    """Create sample donor data for testing"""
    np.random.seed(42)
    n_donors = 100
    
    donors = pd.DataFrame({
        'donor_id': range(1, n_donors + 1),
        'first_donation': pd.date_range(
            start='2020-01-01',
            periods=n_donors,
            freq='D'
        ),
        'frequency': np.random.poisson(lam=3, size=n_donors),
        'total_amount': np.random.gamma(shape=2, scale=100, size=n_donors),
        'engagement_score': np.random.beta(2, 5, size=n_donors),
        'wealth_score': np.random.beta(2, 5, size=n_donors),
        'recency_days': np.random.randint(1, 365, size=n_donors)
    })
    
    donations = []
    for _, donor in donors.iterrows():
        n_donations = donor['frequency']
        donation_dates = pd.date_range(
            start=donor['first_donation'],
            periods=n_donations,
            freq='M'
        )
        
        for date in donation_dates:
            donations.append({
                'donor_id': donor['donor_id'],
                'donation_date': date,
                'amount': np.random.gamma(shape=2, scale=100)
            })
    
    donations = pd.DataFrame(donations)
    
    return {
        'donors': donors,
        'donations': donations
    }

def test_ltv_model(sample_data):
    """Test Donor Lifetime Value model"""
    ltv_model = DonorLifetimeValue(prediction_horizon=180)
    
    # Prepare data
    X = ltv_model.prepare_target(
        sample_data['donations'],
        sample_data['donors']
    )
    
    y = X.pop('future_ltv')
    
    # Test model fitting
    ltv_model.fit(X, y)
    
    # Test predictions
    predictions = ltv_model.predict(X)
    assert len(predictions) == len(X)
    assert all(predictions >= 0)  # LTV should be non-negative
    
    # Test SHAP explanations
    explanations = ltv_model.explain_predictions(X)
    assert 'shap_values' in explanations
    assert 'feature_names' in explanations
    
def test_segmentation(sample_data):
    """Test donor segmentation"""
    segmentation = DonorSegmentation(n_clusters=3)
    
    # Test fitting
    segmentation.fit(sample_data['donors'])
    
    # Test predictions
    segments = segmentation.predict(sample_data['donors'])
    assert len(segments) == len(sample_data['donors'])
    assert len(np.unique(segments)) == 3
    
    # Test segment analysis
    analysis = segmentation.analyze_segments(sample_data['donors'])
    assert len(analysis) == 3
    assert 'pct_donors' in analysis.columns.get_level_values(0)
    assert 'pct_revenue' in analysis.columns.get_level_values(0)
    
def test_churn_prediction(sample_data):
    """Test churn prediction"""
    churn_model = DonorChurnPrediction(churn_threshold_days=180)
    
    # Prepare target
    y = churn_model.prepare_churn_target(
        sample_data['donations']
    )
    
    # Test model fitting
    churn_model.fit(sample_data['donors'], y)
    
    # Test predictions
    proba = churn_model.predict_proba(sample_data['donors'])
    assert len(proba) == len(sample_data['donors'])
    assert all((proba >= 0) & (proba <= 1))
    
    # Test explanations
    explanations = churn_model.explain_predictions(sample_data['donors'])
    assert 'shap_values' in explanations
    assert 'feature_names' in explanations