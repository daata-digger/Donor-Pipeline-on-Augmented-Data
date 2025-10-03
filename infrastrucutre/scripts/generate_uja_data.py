"""
Script to generate realistic donor data for UJA Federation of NY
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pathlib

# Common Jewish and general surnames
surnames = [
    'Cohen', 'Levy', 'Goldberg', 'Shapiro', 'Rosenberg', 'Steinberg', 'Friedman', 
    'Kaplan', 'Schwartz', 'Bernstein', 'Klein', 'Abrams', 'Roth', 'Goldman', 'Stern',
    'Blum', 'Weinstein', 'Katz', 'Greenberg', 'Silver', 'Bloomberg', 'Rothschild',
    'Smith', 'Johnson', 'Williams', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore',
    'Taylor', 'Anderson', 'Thomas', 'White', 'Harris', 'Martin', 'Thompson', 'Garcia'
]

# Common Jewish and general first names
male_first_names = [
    'David', 'Michael', 'Daniel', 'Benjamin', 'Samuel', 'Jacob', 'Ethan', 'Noah',
    'Aaron', 'Joshua', 'Adam', 'Isaac', 'Joseph', 'Nathan', 'Simon', 'Robert',
    'William', 'James', 'John', 'Richard', 'Charles', 'Thomas', 'Steven', 'Mark'
]

female_first_names = [
    'Sarah', 'Rachel', 'Rebecca', 'Hannah', 'Leah', 'Esther', 'Ruth', 'Miriam',
    'Naomi', 'Deborah', 'Elizabeth', 'Jennifer', 'Michelle', 'Linda', 'Barbara',
    'Susan', 'Margaret', 'Lisa', 'Nancy', 'Karen', 'Betty', 'Helen', 'Sandra'
]

# NY area cities and their rough coordinates
cities = {
    'New York': (40.7128, -74.0060),
    'Brooklyn': (40.6782, -73.9442),
    'Queens': (40.7282, -73.7949),
    'Great Neck': (40.8001, -73.7285),
    'Scarsdale': (40.9885, -73.8382),
    'White Plains': (41.0340, -73.7629),
    'Lawrence': (40.6126, -73.7293),
    'Cedarhurst': (40.6212, -73.7268),
    'Roslyn': (40.8000, -73.6515),
    'Syosset': (40.8262, -73.5015),
    'Westchester': (41.1220, -73.7949),
    'Riverdale': (40.8962, -73.9037),
    'Teaneck': (40.8843, -74.0120),
    'Englewood': (40.8928, -73.9726),
    'Deal': (40.2540, -73.9935)
}

# Generate donor data
num_donors = 30000
np.random.seed(42)

donors = []
for i in range(num_donors):
    # Randomly select gender and corresponding first name
    gender = np.random.choice(['M', 'F'])
    if gender == 'M':
        first_name = np.random.choice(male_first_names)
    else:
        first_name = np.random.choice(female_first_names)
    
    # Select location
    city = np.random.choice(list(cities.keys()))
    base_lat, base_lon = cities[city]
    
    # Add small random offset to coordinates
    lat = base_lat + np.random.normal(0, 0.01)
    lon = base_lon + np.random.normal(0, 0.01)
    
    # Generate wealth indicators (log-normal distribution)
    wealth_score = int(np.random.lognormal(10, 1))
    
    donors.append({
        'donor_id': i + 1,
        'first_name': first_name,
        'last_name': np.random.choice(surnames),
        'gender': gender,
        'city': city,
        'state': 'NY' if city != 'Deal' else 'NJ',
        'latitude': lat,
        'longitude': lon,
        'wealth_score': wealth_score
    })

donors_df = pd.DataFrame(donors)

# Generate campaign data
campaigns = [
    {'campaign_id': 1, 'name': 'Annual Campaign 2023', 'type': 'Annual'},
    {'campaign_id': 2, 'name': 'Israel Emergency Fund', 'type': 'Emergency'},
    {'campaign_id': 3, 'name': 'Community Center Capital Campaign', 'type': 'Capital'},
    {'campaign_id': 4, 'name': 'Young Leadership Initiative', 'type': 'Program'},
    {'campaign_id': 5, 'name': 'Jewish Education Fund', 'type': 'Program'},
    {'campaign_id': 6, 'name': 'Senior Care Support', 'type': 'Program'},
    {'campaign_id': 7, 'name': 'Annual Campaign 2024', 'type': 'Annual'},
    {'campaign_id': 8, 'name': 'Synagogue Renovation Fund', 'type': 'Capital'},
    {'campaign_id': 9, 'name': 'Ukraine Relief Fund', 'type': 'Emergency'},
    {'campaign_id': 10, 'name': 'Next Gen Engagement', 'type': 'Program'}
]

campaigns_df = pd.DataFrame(campaigns)

# Generate donation data
start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 9, 30)
date_range = (end_date - start_date).days

donations = []
for donor in donors:
    # Number of donations follows Poisson distribution
    num_donations = np.random.poisson(3)  # Average 3 donations per donor
    
    for _ in range(num_donations):
        donation_date = start_date + timedelta(days=np.random.randint(0, date_range))
        
        # Amount based on wealth score with some randomness
        base_amount = donor['wealth_score'] * 10
        amount = int(np.random.lognormal(np.log(base_amount), 0.5))
        
        # Select campaign - weight toward annual campaigns
        campaign_weights = [3 if c['type'] == 'Annual' else 1 for c in campaigns]
        campaign_id = np.random.choice(campaigns_df['campaign_id'], p=np.array(campaign_weights)/sum(campaign_weights))
        
        donations.append({
            'donation_id': len(donations) + 1,
            'donor_id': donor['donor_id'],
            'campaign_id': campaign_id,
            'amount': amount,
            'donation_date': donation_date
        })

donations_df = pd.DataFrame(donations)

# Save files
root = pathlib.Path(__file__).resolve().parents[1]
data_dir = root / 'data/raw'

donors_df.to_csv(data_dir / 'donors.csv', index=False)
campaigns_df.to_csv(data_dir / 'campaigns.csv', index=False)
donations_df.to_csv(data_dir / 'donations.csv', index=False)

print(f"Created {len(donors_df)} donors")
print(f"Created {len(campaigns_df)} campaigns")
print(f"Created {len(donations_df)} donations")