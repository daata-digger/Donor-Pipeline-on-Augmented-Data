"""
Script to prepare scored donors dataset
"""
import pandas as pd
import pathlib
import numpy as np

root = pathlib.Path(__file__).resolve().parents[1]
raw_dir = root / 'data/raw'
processed_dir = root / 'data/processed'

if not processed_dir.exists():
    processed_dir.mkdir(parents=True)

# Load raw data
donors = pd.read_csv(raw_dir / 'donors.csv')
donations = pd.read_csv(raw_dir / 'donations.csv')
events = pd.read_csv(raw_dir / 'engagement_events.csv')

# Add mock geo coordinates for demo
donors['latitude'] = donors.apply(lambda x: 40.7128 + np.random.normal(0, 2), axis=1)  # Centered on NYC
donors['longitude'] = donors.apply(lambda x: -74.0060 + np.random.normal(0, 2), axis=1)

# Calculate donor features
donor_features = pd.DataFrame()
donor_features['donor_id'] = donors['donor_id']
donor_features['first_name'] = donors.get('first_name', pd.Series(f'Donor_{i}' for i in range(len(donors))))
donor_features['last_name'] = donors.get('last_name', pd.Series(''))
donor_features['city'] = donors.get('city', pd.Series('New York'))
donor_features['state'] = donors.get('state', pd.Series('NY'))
donor_features['latitude'] = donors['latitude']
donor_features['longitude'] = donors['longitude']

# Aggregate donations
donation_aggs = donations.groupby('donor_id').agg({
    'amount': ['sum', 'mean', 'count'],
    'donation_date': ['min', 'max']
}).reset_index()

donation_aggs.columns = ['donor_id', 'total_amount', 'avg_amount', 'frequency', 'first_gift', 'last_gift']
donation_aggs['first_gift'] = pd.to_datetime(donation_aggs['first_gift'])
donation_aggs['last_gift'] = pd.to_datetime(donation_aggs['last_gift'])
donation_aggs['recency_days'] = (pd.Timestamp.now() - donation_aggs['last_gift']).dt.days

# Aggregate events
event_counts = events.groupby('donor_id').size().reset_index(name='events_attended')

# Merge features
donor_features = donor_features.merge(donation_aggs, on='donor_id', how='left')
donor_features = donor_features.merge(event_counts, on='donor_id', how='left')

# Fill missing values
donor_features = donor_features.fillna({
    'total_amount': 0,
    'avg_amount': 0,
    'frequency': 0,
    'events_attended': 0,
    'recency_days': 999999
})

# Calculate simple propensity score (for demo)
donor_features['propensity'] = (
    0.4 * (donor_features['total_amount'] / donor_features['total_amount'].max()) +
    0.3 * (donor_features['frequency'] / donor_features['frequency'].max()) +
    0.2 * (1 - donor_features['recency_days'] / donor_features['recency_days'].max()) +
    0.1 * (donor_features['events_attended'] / donor_features['events_attended'].max())
)

# Calculate deciles
donor_features['decile'] = pd.qcut(donor_features['propensity'], q=10, labels=range(1, 11))

# Save processed data
donor_features.to_csv(processed_dir / 'scored_donors.csv', index=False)
print("Created scored_donors.csv")