# scripts/local_curate.py  (VERBOSE + project-root paths)
import pandas as pd
import pathlib
from datetime import datetime

# __file__ = ...\donor-analytics-enterprise\scripts\local_curate.py
# parents[0]=scripts, [1]=donor-analytics-enterprise, [2]=<PROJECT ROOT>
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
CUR = PROJECT_ROOT / "data" / "processed" / "curated"

print(f"[{datetime.now()}] PROJECT_ROOT: {PROJECT_ROOT}")
print(f"[{datetime.now()}] RAW: {RAW}")
print(f"[{datetime.now()}] CURATED: {CUR}")

required = ["donors.csv","donations.csv","engagement_events.csv","wealth_external.csv"]
missing = [f for f in required if not (RAW / f).exists()]
if missing:
    raise FileNotFoundError(f"Missing raw files at {RAW}: {missing}")

print(f"[{datetime.now()}] Reading donors...")
donors = pd.read_csv(RAW/"donors.csv", parse_dates=["join_date"])
print(f"  donors: {len(donors):,}")

print(f"[{datetime.now()}] Reading donations...")
donations = pd.read_csv(RAW/"donations.csv", parse_dates=["donation_date"])
print(f"  donations: {len(donations):,}")

print(f"[{datetime.now()}] Reading engagement & wealth...")
events = pd.read_csv(RAW/"engagement_events.csv")
wealth = pd.read_csv(RAW/"wealth_external.csv")

today = pd.Timestamp("2025-10-03")
print(f"[{datetime.now()}] Aggregating donations (R/F/recency)...")
agg = (
    donations.assign(days_since=lambda d: (today - d["donation_date"]).dt.days)
    .groupby("donor_id", as_index=False)
    .agg(
        total_amount=("amount","sum"),
        frequency=("donation_id","count"),
        last_gift=("donation_date","max"),
        recency_days=("days_since","min"),
    )
)

print(f"[{datetime.now()}] Joining features...")
features = (
    donors.merge(agg, on="donor_id", how="left")
          .merge(events, on="donor_id", how="left")
          .merge(wealth[["donor_id","wealth_score_ext"]], on="donor_id", how="left")
)

print(f"[{datetime.now()}] Filling NAs and writing outputs...")
features = features.fillna({
    "total_amount":0, "frequency":0, "recency_days":9999,
    "events_attended":0, "volunteer_hours":0, "wealth_score_ext":0
})

CUR.mkdir(parents=True, exist_ok=True)
features.to_csv(CUR/"donor_features.csv", index=False)
donations.to_csv(CUR/"fact_donation.csv", index=False)
donors.to_csv(CUR/"dim_donor.csv", index=False)

print(f"[{datetime.now()}] Curated written → {CUR}")
