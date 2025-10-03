"""
Script to generate proper engagement events data
"""
import pandas as pd
import numpy as np
import pathlib
from datetime import datetime, timedelta

# Set up paths
root = pathlib.Path(__file__).resolve().parents[1]
raw_data = root / 'data/raw'

# Load donors
donors = pd.read_csv(raw_data / 'donors.csv')

# Create events data
start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 9, 30)
date_range = (end_date - start_date).days

events = []
for donor_id in donors['donor_id'].unique():
    # Randomly decide how many events this donor attended (0-10)
    num_events = np.random.poisson(2)  # Average of 2 events per donor
    if num_events > 0:
        # Generate random dates for each event
        for _ in range(num_events):
            event_date = start_date + timedelta(days=np.random.randint(0, date_range))
            event_type = np.random.choice(['Gala', 'Workshop', 'Volunteer Day', 'Community Meeting', 'Fundraiser'])
            hours = np.random.uniform(1, 8) if event_type == 'Volunteer Day' else 0
            
            events.append({
                'donor_id': donor_id,
                'event_date': event_date,
                'event_type': event_type,
                'volunteer_hours': hours
            })

# Convert to DataFrame
events_df = pd.DataFrame(events)
events_df = events_df.sort_values(['donor_id', 'event_date'])

# Save to CSV
events_df.to_csv(raw_data / 'engagement_events.csv', index=False)
print("Created new engagement_events.csv with proper event dates")