"""
Script to generate UJA Federation engagement events
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pathlib

# Set up paths
root = pathlib.Path(__file__).resolve().parents[1]
raw_data = root / 'data/raw'

# Load donors
donors = pd.read_csv(raw_data / 'donors.csv')

# Event types and their descriptions
event_types = [
    'Annual Gala',
    'Leadership Breakfast',
    'Young Professional Networking',
    'Community Service Day',
    'Israel Advocacy Workshop',
    'Women\'s Philanthropy Lunch',
    'NextGen Leadership Series',
    'Donor Recognition Event',
    'Holiday Community Celebration',
    'Educational Symposium'
]

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
            event_type = np.random.choice(event_types)
            # More volunteer hours for community service events
            hours = np.random.uniform(2, 6) if event_type == 'Community Service Day' else 0
            
            events.append({
                'donor_id': donor_id,
                'event_date': event_date,
                'event_type': event_type,
                'volunteer_hours': hours
            })

# Convert to DataFrame and sort
events_df = pd.DataFrame(events)
events_df = events_df.sort_values(['donor_id', 'event_date'])

# Save to CSV
events_df.to_csv(raw_data / 'engagement_events.csv', index=False)
print(f"Created {len(events_df)} event records")