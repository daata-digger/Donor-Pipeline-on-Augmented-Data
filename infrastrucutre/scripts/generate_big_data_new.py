# generate_big_data.py
"""
Sophisticated data generator for donor analytics with realistic patterns:
- Seasonal campaign patterns
- Correlated wealth indicators
- Engagement patterns
- Giving behavior segments
"""
import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
import pathlib
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--donors", type=int, default=100000, help="Number of donor records to generate")
parser.add_argument("--years", type=int, default=10, help="Years of historical data")
parser.add_argument("--out", type=str, default="data/raw", help="Output directory")
args = parser.parse_args()

fake = Faker('en_US')  # Changed to US locale
np.random.seed(42)
Faker.seed(42)
random.seed(42)

CAMPAIGN_TYPES = [
    'Annual Fund',
    'Capital Campaign', 
    'Emergency Relief',
    'Planned Giving',
    'Special Projects',
    'Endowment',
    'Scholarship Fund'
]

ENGAGEMENT_TYPES = [
    'Gala Attendance',
    'Volunteer Work',
    'Leadership Council',
    'Donor Circle',
    'Site Visit',
    'Workshop Participation',
    'Committee Member'
]

PAYMENT_METHODS = [
    'Credit Card',
    'Check',
    'Wire Transfer',
    'PayPal',
    'Donor Advised Fund',
    'Stock Transfer'
]

SOURCE_CHANNELS = [
    'Website',
    'Direct Mail',
    'Event',
    'Referral',
    'Board Member',
    'Social Media',
    'Phone Campaign'
]

def generate_donors(n_donors):
    """Generate sophisticated donor profiles"""
    data = []
    for i in range(n_donors):
        join_date = fake.date_between(start_date='-10y', end_date='today')
        data.append({
            'donor_id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip': fake.zipcode(),
            'join_date': join_date,
            'age': int(np.random.normal(55, 15).clip(25, 90)),
            'occupation': fake.job(),
            'source': np.random.choice(SOURCE_CHANNELS, p=[0.3, 0.2, 0.15, 0.15, 0.1, 0.05, 0.05])
        })
    return pd.DataFrame(data)

def generate_campaigns(n_years):
    """Generate fundraising campaigns with seasonal patterns"""
    data = []
    campaign_id = 1
    
    for year in range(2025 - n_years, 2026):
        # Annual fund campaigns
        for season, quarter in [('Spring', 1), ('Summer', 2), ('Fall', 3), ('Winter', 4)]:
            start_date = fake.date_between(
                start_date=f'{year}-{quarter*3-2}-01', 
                end_date=f'{year}-{quarter*3}-28'
            )
            end_date = fake.date_between(
                start_date=start_date,
                end_date=f'{year}-{quarter*3}-28'
            )
            data.append({
                'campaign_id': campaign_id,
                'name': f"{season} Annual Fund {year}",
                'type': 'Annual Fund',
                'start_date': start_date,
                'end_date': end_date,
                'target_amount': int(np.random.lognormal(11.5, 1))
            })
            campaign_id += 1
        
        # Special campaigns
        for _ in range(random.randint(2, 3)):
            campaign_type = random.choice([t for t in CAMPAIGN_TYPES if t != 'Annual Fund'])
            start_date = fake.date_between(start_date=f'{year}-01-01', end_date=f'{year}-12-31')
            data.append({
                'campaign_id': campaign_id,
                'name': f"{campaign_type} {year}",
                'type': campaign_type,
                'start_date': start_date,
                'end_date': fake.date_between(
                    start_date=start_date,
                    end_date=f'{year}-12-31'
                ),
                'target_amount': int(np.random.lognormal(12, 1.2))
            })
            campaign_id += 1
            
    return pd.DataFrame(data)

def generate_donations(donors_df, campaigns_df):
    """Generate donations with realistic patterns"""
    # Identify high-value donors (20% of base)
    high_value_donors = np.random.choice(
        donors_df['donor_id'].values, 
        size=int(len(donors_df)*0.2), 
        replace=False
    )
    
    data = []
    donation_id = 1
    
    for _, donor in donors_df.iterrows():
        # Determine donor's giving pattern
        is_high_value = donor['donor_id'] in high_value_donors
        n_donations = np.random.poisson(8 if is_high_value else 3)
        
        for _ in range(n_donations):
            campaign = campaigns_df.sample(n=1).iloc[0]
            
            # Amount based on donor segment and campaign type
            base_amount = np.random.lognormal(
                8 if is_high_value else 7,
                1.2
            )
            
            # Adjust amount based on campaign type
            multiplier = {
                'Capital Campaign': 2.0,
                'Endowment': 1.8,
                'Annual Fund': 1.0,
                'Emergency Relief': 1.2
            }.get(campaign['type'], 1.0)
            
            amount = int(base_amount * multiplier)
            
            # Add some major gifts
            if is_high_value and random.random() < 0.1:
                amount *= random.randint(5, 10)
            
            # Date within campaign period
            donation_date = fake.date_between(
                start_date=campaign['start_date'],
                end_date=campaign['end_date']
            )
            
            data.append({
                'donation_id': donation_id,
                'donor_id': donor['donor_id'],
                'campaign_id': campaign['campaign_id'],
                'amount': amount,
                'donation_date': donation_date,
                'payment_method': np.random.choice(
                    PAYMENT_METHODS,
                    p=[0.4, 0.25, 0.15, 0.1, 0.05, 0.05]
                )
            })
            donation_id += 1
    
    return pd.DataFrame(data)

def generate_engagement(donors_df):
    """Generate sophisticated engagement patterns"""
    data = []
    event_id = 1
    
    # More engaged donors (30% of base)
    engaged_donors = np.random.choice(
        donors_df['donor_id'].values,
        size=int(len(donors_df)*0.3),
        replace=False
    )
    
    for _, donor in donors_df.iterrows():
        is_engaged = donor['donor_id'] in engaged_donors
        n_events = np.random.poisson(6 if is_engaged else 2)
        
        for _ in range(n_events):
            event_type = np.random.choice(ENGAGEMENT_TYPES)
            event_date = fake.date_between(start_date='-5y', end_date='today')
            
            data.append({
                'event_id': event_id,
                'donor_id': donor['donor_id'],
                'event_type': event_type,
                'event_date': event_date,
                'hours': int(np.random.exponential(3)) if event_type == 'Volunteer Work' else 0,
                'leadership_role': random.random() < 0.2 if is_engaged else False
            })
            event_id += 1
    
    return pd.DataFrame(data)

def generate_wealth_data(donors_df):
    """Generate correlated wealth indicators"""
    n_donors = len(donors_df)
    
    # Generate base wealth score with beta distribution
    base_wealth = np.random.beta(2, 5, n_donors)
    
    # Create correlated metrics
    data = {
        'donor_id': donors_df['donor_id'],
        'real_estate_value': np.random.lognormal(12 + base_wealth*2, 1, n_donors),
        'stock_holdings': np.random.lognormal(10 + base_wealth*3, 2, n_donors),
        'income_estimate': np.random.lognormal(11 + base_wealth, 0.5, n_donors),
        'philanthropic_score': (base_wealth * 0.7 + np.random.beta(2, 5, n_donors) * 0.3) * 100,
        'wealth_score_ext': base_wealth * 100
    }
    return pd.DataFrame(data)

def main():
    print(f"Generating {args.donors:,} donor records with {args.years} years of history...")
    
    # Create output directory
    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate all datasets
    print("Generating donors...")
    donors = generate_donors(args.donors)
    donors.to_csv(out_dir/'donors.csv', index=False)
    
    print("Generating campaigns...")
    campaigns = generate_campaigns(args.years)
    campaigns.to_csv(out_dir/'campaigns.csv', index=False)
    
    print("Generating donations...")
    donations = generate_donations(donors, campaigns)
    donations.to_csv(out_dir/'donations.csv', index=False)
    
    print("Generating engagement events...")
    events = generate_engagement(donors)
    events.to_csv(out_dir/'engagement_events.csv', index=False)
    
    print("Generating wealth data...")
    wealth = generate_wealth_data(donors)
    wealth.to_csv(out_dir/'wealth_external.csv', index=False)
    
    print("\nData generation complete! Summary:")
    print(f"- Donors: {len(donors):,}")
    print(f"- Campaigns: {len(campaigns):,}")
    print(f"- Donations: {len(donations):,}")
    print(f"- Events: {len(events):,}")
    print(f"\nFiles written to: {out_dir}")

if __name__ == '__main__':
    main()