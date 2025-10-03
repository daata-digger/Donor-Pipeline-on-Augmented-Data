"""
Campaign Simulator Module

This module provides sophisticated campaign simulation capabilities for donor analytics:
- Campaign outcome prediction
- Donor targeting and segmentation
- Gift size estimation
- Response probability calculation
- Campaign planning and strategy generation
"""
import pandas as pd
import numpy as np
from datetime import datetime
import pathlib

class CampaignSimulator:
    def __init__(self, donors_df, donations_df):
        self.donors = donors_df
        self.donations = donations_df
        
    def simulate_campaign(self, target_segments, campaign_type, goal_amount):
        """
        Simulate a campaign's potential outcomes
        """
        # Filter donors in target segments
        target_donors = self.donors[self.donors['segment'].isin(target_segments)]
        
        # Calculate potential based on propensity and historical giving
        target_donors['expected_gift'] = target_donors.apply(
            lambda x: self._calculate_expected_gift(x, campaign_type),
            axis=1
        )
        
        # Calculate response probability
        target_donors['response_prob'] = target_donors.apply(
            lambda x: self._calculate_response_prob(x, campaign_type),
            axis=1
        )
        
        # Calculate expected value
        target_donors['expected_value'] = (
            target_donors['expected_gift'] * target_donors['response_prob']
        )
        
        # Sort by expected value
        target_donors = target_donors.sort_values('expected_value', ascending=False)
        
        # Calculate cumulative potential
        target_donors['cumulative_potential'] = target_donors['expected_value'].cumsum()
        
        # Find optimal number of donors to reach goal
        donors_needed = len(target_donors[target_donors['cumulative_potential'] <= goal_amount]) + 1
        
        return {
            'target_donors': target_donors.head(donors_needed),
            'total_potential': target_donors['expected_value'].sum(),
            'donors_needed': donors_needed,
            'avg_gift': target_donors['expected_gift'].mean(),
            'response_rate': target_donors['response_prob'].mean()
        }
    
    def _calculate_expected_gift(self, donor, campaign_type):
        """
        Calculate expected gift amount based on donor history and campaign type
        """
        donor_gifts = self.donations[self.donations['donor_id'] == donor['donor_id']]
        
        if len(donor_gifts) == 0:
            return donor['wealth_score'] * 1000  # Base amount on wealth score
            
        base_amount = donor_gifts['amount'].mean()
        
        # Adjust based on campaign type
        multipliers = {
            'annual': 1.0,
            'emergency': 1.5,
            'capital': 2.0,
            'endowment': 3.0
        }
        
        # Apply wealth score and campaign multiplier
        return base_amount * multipliers.get(campaign_type, 1.0) * (1 + donor['wealth_score'])
    
    def _calculate_response_prob(self, donor, campaign_type):
        """
        Calculate probability of response based on donor profile and campaign type
        """
        base_prob = donor['propensity']
        
        # Adjust for recency
        recency_factor = np.exp(-donor['recency_days'] / 365)  # Decay factor
        
        # Adjust for campaign type
        campaign_factors = {
            'annual': 1.0,
            'emergency': 0.8,
            'capital': 0.6,
            'endowment': 0.4
        }
        
        return min(base_prob * recency_factor * campaign_factors.get(campaign_type, 1.0), 1.0)
    
    def create_campaign_plan(self, simulation_result):
        """
        Generate actionable campaign plan from simulation
        """
        donors = simulation_result['target_donors']
        
        # Segment donors into tiers
        donors['tier'] = pd.qcut(donors['expected_value'], q=3, labels=['Tier 3', 'Tier 2', 'Tier 1'])
        
        # Generate contact strategy
        donors['strategy'] = donors['tier'].map({
            'Tier 1': 'Personal visit + Custom proposal',
            'Tier 2': 'Phone call + Personalized letter',
            'Tier 3': 'Email + Direct mail'
        })
        
        return donors[['donor_id', 'first_name', 'last_name', 'expected_gift',
                      'response_prob', 'expected_value', 'tier', 'strategy']]