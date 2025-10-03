"""
Core data processing functionality that works with or without cloud providers
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta

class DonorAnalytics:
    def __init__(self, cloud_provider=None):
        self.cloud_provider = cloud_provider
        self.data_dir = Path("data")
        self.processed_dir = self.data_dir / "processed"
        self.raw_dir = self.data_dir / "raw"
        
        # Ensure directories exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self, 
                 donors_path: Union[str, Path],
                 donations_path: Union[str, Path],
                 campaigns_path: Union[str, Path],
                 events_path: Optional[Union[str, Path]] = None,
                 wealth_path: Optional[Union[str, Path]] = None) -> Dict[str, pd.DataFrame]:
        """Load data from files or cloud storage"""
        
        if self.cloud_provider:
            # Download from cloud if paths are cloud URLs
            pass
        
        # Load core datasets
        donors = pd.read_csv(donors_path)
        donations = pd.read_csv(donations_path)
        campaigns = pd.read_csv(campaigns_path)
        
        # Load optional datasets
        events = pd.read_csv(events_path) if events_path else None
        wealth = pd.read_csv(wealth_path) if wealth_path else None
        
        return {
            'donors': donors,
            'donations': donations,
            'campaigns': campaigns,
            'events': events,
            'wealth': wealth
        }

    def compute_rfm_features(self, donors: pd.DataFrame, donations: pd.DataFrame) -> pd.DataFrame:
        """Compute RFM (Recency, Frequency, Monetary) features"""
        
        # Calculate features per donor
        rfm = donations.groupby('donor_id').agg({
            'donation_date': lambda x: (datetime.now() - pd.to_datetime(x.max())).days,  # Recency
            'donation_id': 'count',  # Frequency
            'amount': 'sum'  # Monetary
        }).reset_index()
        
        # Rename columns
        rfm.columns = ['donor_id', 'recency_days', 'frequency', 'total_amount']
        
        # Merge with donor information
        donor_features = donors.merge(rfm, on='donor_id', how='left')
        
        return donor_features

    def add_engagement_features(self, 
                              donor_features: pd.DataFrame,
                              events: pd.DataFrame) -> pd.DataFrame:
        """Add engagement features if available"""
        
        if events is not None:
            donor_features = donor_features.merge(
                events,
                on='donor_id',
                how='left'
            )
            
            # Fill missing values
            donor_features['events_attended'] = donor_features['events_attended'].fillna(0)
            donor_features['volunteer_hours'] = donor_features['volunteer_hours'].fillna(0)
            
            # Compute engagement score
            donor_features['engagement_score'] = (
                0.7 * donor_features['events_attended'] +
                0.3 * donor_features['volunteer_hours']
            ) / (donor_features[['events_attended', 'volunteer_hours']].max().max())
            
        return donor_features

    def add_wealth_features(self,
                          donor_features: pd.DataFrame,
                          wealth: pd.DataFrame) -> pd.DataFrame:
        """Add wealth features if available"""
        
        if wealth is not None:
            donor_features = donor_features.merge(
                wealth,
                on='donor_id',
                how='left'
            )
            
            # Fill missing values with median
            donor_features['wealth_score_ext'] = donor_features['wealth_score_ext'].fillna(
                donor_features['wealth_score_ext'].median()
            )
            
        return donor_features

    def compute_giving_metrics(self,
                             donors: pd.DataFrame,
                             donations: pd.DataFrame,
                             campaigns: pd.DataFrame) -> Dict[str, float]:
        """Compute key giving metrics"""
        
        total_donors = len(donors)
        active_donors = len(donations['donor_id'].unique())
        total_donations = len(donations)
        total_amount = donations['amount'].sum()
        avg_donation = donations['amount'].mean()
        
        # Campaign success rates
        campaign_stats = donations.groupby('campaign_id').agg({
            'amount': ['sum', 'count']
        })
        campaign_stats.columns = ['total_raised', 'num_donations']
        campaign_success = campaign_stats.merge(
            campaigns[['campaign_id', 'goal']],
            left_index=True,
            right_on='campaign_id'
        )
        campaign_success['success_rate'] = campaign_success['total_raised'] / campaign_success['goal']
        
        return {
            'total_donors': total_donors,
            'active_donors': active_donors,
            'donor_activation_rate': active_donors / total_donors,
            'total_donations': total_donations,
            'total_amount': total_amount,
            'avg_donation': avg_donation,
            'campaign_success_rate': campaign_success['success_rate'].mean()
        }

    def save_features(self, donor_features: pd.DataFrame, output_path: Union[str, Path]):
        """Save computed features locally or to cloud"""
        
        if self.cloud_provider:
            # Save to cloud storage
            local_path = self.processed_dir / "donor_features_temp.csv"
            donor_features.to_csv(local_path, index=False)
            self.cloud_provider.upload_file(local_path, output_path)
            local_path.unlink()  # Clean up temp file
        else:
            # Save locally
            donor_features.to_csv(output_path, index=False)

    def process_full_pipeline(self, data_paths: Dict[str, Union[str, Path]]) -> Dict:
        """Run the full data processing pipeline"""
        
        # Load data
        data = self.load_data(**data_paths)
        
        # Compute features
        donor_features = self.compute_rfm_features(data['donors'], data['donations'])
        donor_features = self.add_engagement_features(donor_features, data['events'])
        donor_features = self.add_wealth_features(donor_features, data['wealth'])
        
        # Compute metrics
        metrics = self.compute_giving_metrics(
            data['donors'],
            data['donations'],
            data['campaigns']
        )
        
        # Save results
        output_path = self.processed_dir / "donor_features.csv"
        self.save_features(donor_features, output_path)
        
        return {
            'donor_features': donor_features,
            'metrics': metrics
        }