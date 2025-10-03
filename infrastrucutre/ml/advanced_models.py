"""
Advanced ML Features for Donor Analytics
"""
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from xgboost import XGBRegressor, XGBClassifier
from lightgbm import LGBMRegressor
import shap
from typing import Dict, List, Optional, Tuple, Union
import joblib
from pathlib import Path

class DonorFeatureEngineering(BaseEstimator, TransformerMixin):
    """Custom transformer for donor feature engineering"""
    
    def __init__(self, temporal_features: bool = True, interaction_features: bool = True):
        self.temporal_features = temporal_features
        self.interaction_features = interaction_features
        
    def fit(self, X, y=None):
        return self
        
    def transform(self, X):
        X_transformed = X.copy()
        
        if self.temporal_features:
            # Add temporal features
            X_transformed['days_since_first'] = (
                pd.to_datetime('now') - pd.to_datetime(X_transformed['first_donation'])
            ).dt.days
            
            X_transformed['donation_frequency'] = (
                X_transformed['frequency'] / X_transformed['days_since_first']
            ) * 365  # Annualized frequency
            
        if self.interaction_features:
            # Create interaction features
            X_transformed['wealth_engagement'] = X_transformed['wealth_score'] * X_transformed['engagement_score']
            X_transformed['frequency_monetary'] = X_transformed['frequency'] * X_transformed['total_amount']
            
        return X_transformed

class DonorLifetimeValue:
    """Predict donor lifetime value using advanced ML techniques"""
    
    def __init__(self, 
                 prediction_horizon: int = 365,
                 feature_engineering: bool = True,
                 model_type: str = 'xgboost'):
        self.prediction_horizon = prediction_horizon
        self.feature_engineering = feature_engineering
        self.model_type = model_type
        
        # Initialize pipeline components
        self.feature_engineer = DonorFeatureEngineering()
        self.scaler = StandardScaler()
        
        # Select model
        if model_type == 'xgboost':
            self.model = XGBRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=4,
                objective='reg:squarederror'
            )
        elif model_type == 'lightgbm':
            self.model = LGBMRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=4
            )
            
        # Create pipeline
        self.pipeline = Pipeline([
            ('feature_engineering', self.feature_engineer),
            ('scaler', self.scaler),
            ('model', self.model)
        ])
        
        # Initialize SHAP explainer
        self.explainer = None
        
    def prepare_target(self, 
                      donations: pd.DataFrame,
                      donor_features: pd.DataFrame) -> pd.DataFrame:
        """Prepare LTV target variable"""
        
        # Calculate future donations for each donor
        future_donations = donations[
            pd.to_datetime(donations['donation_date']) > 
            (pd.to_datetime('now') - pd.Timedelta(days=self.prediction_horizon))
        ]
        
        future_ltv = future_donations.groupby('donor_id')['amount'].sum().reset_index()
        future_ltv.columns = ['donor_id', 'future_ltv']
        
        # Merge with donor features
        X = donor_features.merge(future_ltv, on='donor_id', how='left')
        X['future_ltv'] = X['future_ltv'].fillna(0)
        
        return X
        
    def fit(self, 
            X: pd.DataFrame,
            y: pd.Series,
            eval_set: Optional[Tuple[pd.DataFrame, pd.Series]] = None) -> None:
        """Train the LTV model"""
        
        self.pipeline.fit(X, y)
        
        # Initialize SHAP explainer
        self.explainer = shap.TreeExplainer(self.pipeline.named_steps['model'])
        
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Predict donor LTV"""
        return self.pipeline.predict(X)
        
    def explain_predictions(self, 
                          X: pd.DataFrame,
                          donor_ids: Optional[List[int]] = None) -> Dict:
        """Generate SHAP explanations for predictions"""
        
        if donor_ids is not None:
            X = X[X['donor_id'].isin(donor_ids)]
            
        shap_values = self.explainer.shap_values(
            self.pipeline.named_steps['feature_engineering'].transform(X)
        )
        
        return {
            'shap_values': shap_values,
            'feature_names': X.columns,
            'donor_ids': X['donor_id'].values
        }
        
    def save_model(self, path: Union[str, Path]) -> None:
        """Save model and pipeline to disk"""
        joblib.dump(self.pipeline, path)
        
    @classmethod
    def load_model(cls, path: Union[str, Path]) -> 'DonorLifetimeValue':
        """Load model and pipeline from disk"""
        instance = cls()
        instance.pipeline = joblib.load(path)
        instance.model = instance.pipeline.named_steps['model']
        instance.explainer = shap.TreeExplainer(instance.model)
        return instance

class DonorSegmentation:
    """Advanced donor segmentation using multiple techniques"""
    
    def __init__(self,
                 n_clusters: int = 5,
                 method: str = 'kmeans',
                 features: Optional[List[str]] = None):
        self.n_clusters = n_clusters
        self.method = method
        self.features = features or [
            'recency_days', 'frequency', 'monetary',
            'engagement_score', 'wealth_score'
        ]
        
        # Initialize models
        if method == 'kmeans':
            from sklearn.cluster import KMeans
            self.model = KMeans(n_clusters=n_clusters, random_state=42)
        elif method == 'gmm':
            from sklearn.mixture import GaussianMixture
            self.model = GaussianMixture(n_components=n_clusters, random_state=42)
        elif method == 'hierarchical':
            from sklearn.cluster import AgglomerativeClustering
            self.model = AgglomerativeClustering(n_clusters=n_clusters)
            
        self.scaler = StandardScaler()
        
    def fit(self, X: pd.DataFrame) -> None:
        """Fit segmentation model"""
        X_scaled = self.scaler.fit_transform(X[self.features])
        self.model.fit(X_scaled)
        
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Predict segments for donors"""
        X_scaled = self.scaler.transform(X[self.features])
        return self.model.predict(X_scaled)
        
    def analyze_segments(self, X: pd.DataFrame) -> pd.DataFrame:
        """Generate segment analysis"""
        X['segment'] = self.predict(X)
        
        segment_analysis = X.groupby('segment').agg({
            'donor_id': 'count',
            'monetary': ['mean', 'sum'],
            'frequency': 'mean',
            'recency_days': 'mean',
            'engagement_score': 'mean',
            'wealth_score': 'mean'
        }).round(2)
        
        # Calculate segment metrics
        segment_analysis['pct_donors'] = (
            segment_analysis[('donor_id', 'count')] / len(X) * 100
        ).round(2)
        
        segment_analysis['pct_revenue'] = (
            segment_analysis[('monetary', 'sum')] / X['monetary'].sum() * 100
        ).round(2)
        
        return segment_analysis

class DonorChurnPrediction:
    """Predict donor churn probability"""
    
    def __init__(self, 
                 churn_threshold_days: int = 365,
                 feature_engineering: bool = True):
        self.churn_threshold_days = churn_threshold_days
        self.feature_engineering = feature_engineering
        
        # Initialize pipeline components
        self.feature_engineer = DonorFeatureEngineering()
        self.scaler = StandardScaler()
        self.model = XGBClassifier(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=4,
            objective='binary:logistic'
        )
        
        # Create pipeline
        self.pipeline = Pipeline([
            ('feature_engineering', self.feature_engineer),
            ('scaler', self.scaler),
            ('model', self.model)
        ])
        
        # Initialize SHAP explainer
        self.explainer = None
        
    def prepare_churn_target(self,
                           donations: pd.DataFrame,
                           analysis_date: Optional[str] = None) -> pd.Series:
        """Prepare churn target variable"""
        if analysis_date is None:
            analysis_date = pd.Timestamp.now()
        else:
            analysis_date = pd.to_datetime(analysis_date)
            
        last_donation = donations.groupby('donor_id')['donation_date'].max()
        days_since_donation = (analysis_date - pd.to_datetime(last_donation)).dt.days
        
        return (days_since_donation > self.churn_threshold_days).astype(int)
        
    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Train the churn prediction model"""
        self.pipeline.fit(X, y)
        self.explainer = shap.TreeExplainer(self.pipeline.named_steps['model'])
        
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Predict churn probability"""
        return self.pipeline.predict_proba(X)[:, 1]
        
    def explain_predictions(self, X: pd.DataFrame) -> Dict:
        """Generate SHAP explanations for predictions"""
        shap_values = self.explainer.shap_values(
            self.pipeline.named_steps['feature_engineering'].transform(X)
        )
        
        return {
            'shap_values': shap_values,
            'feature_names': X.columns
        }