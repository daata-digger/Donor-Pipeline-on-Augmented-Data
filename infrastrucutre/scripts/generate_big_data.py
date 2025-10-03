# generate_big_data.py
# Sophisticated data generator for donor analytics
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

fake = Faker('en_IN')
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

donors = []
donors = generate_donors(args.donors)
out_dir = pathlib.Path(args.out)
out_dir.mkdir(parents=True, exist_ok=True)

print("Saving donors...")
donors.to_csv(out_dir/'donors.csv', index=False)

print("Generating campaigns...")
camps = []
for y in years:
    for c in ["Annual","Emergency Appeal","Education","Health"]:
        start = dt.date(y, random.randint(1,3), random.randint(1,28))
        end = dt.date(y, min(12,start.month+random.randint(3,6)), random.randint(1,28))
        camps.append({"campaign_id":f"{y}-{c.replace(' ','_')}", "name":c,
                      "start_date":start.isoformat(),"end_date":end.isoformat(),"fy":f"FY{str(y)[-2:]}"})
pd.DataFrame(camps).to_csv(os.path.join(args.out,"campaigns.csv"), index=False)

# donations
donations=[]; did=1
for d in donors:
    lam = 0.3 + 3.0*float(d["engagement_index"]) + 1.0*float(d["wealth_index"])
    n = np.random.poisson(lam=lam)+1
    for _ in range(n):
        y=random.choice(years); m=random.randint(1,12); day=random.randint(1,28)
        base=math.exp(np.random.normal(7.5,0.9))
        amt=base*(1+2*float(d["wealth_index"]))*(0.6+0.8*float(d["engagement_index"]))
        if random.random()<0.02*(1+float(d["wealth_index"])): amt*=10
        donations.append({"donation_id":did,"donor_id":d["donor_id"],
                          "campaign_id":random.choice(camps)["campaign_id"],
                          "program":"Annual","amount":round(float(amt),2),
                          "donation_date":dt.date(y,m,day).isoformat()}); did+=1
pd.DataFrame(donations).to_csv(os.path.join(args.out,"donations.csv"), index=False)

# engagement & wealth
pd.DataFrame([{"donor_id":d["donor_id"],
               "events_attended":int(np.random.binomial(1, 0.25+0.5*float(d["engagement_index"])))*random.randint(0,5),
               "volunteer_hours":max(0, np.random.normal(10*float(d["engagement_index"]),5))}
              for d in donors]).to_csv(os.path.join(args.out,"engagement_events.csv"), index=False)

pd.DataFrame([{"donor_id":d["donor_id"],
               "wealth_score_ext":float(np.clip(np.random.normal(float(d["wealth_index"]),0.15),0,1))}
              for d in donors]).to_csv(os.path.join(args.out,"wealth_external.csv"), index=False)

print("Big data generated in", args.out)
