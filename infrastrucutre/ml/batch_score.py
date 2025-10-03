import pandas as pd, pathlib, joblib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
ENT_ROOT = PROJECT_ROOT/'donor-analytics-enterprise'
features = pd.read_csv(ENT_ROOT/'data/processed/curated/donor_features.csv')
model = joblib.load(ENT_ROOT/'ml/model_xgb.pkl')

X = features[['frequency','total_amount','recency_days','events_attended','volunteer_hours','wealth_score_ext']].copy()
X['recency_days'] = X['recency_days'].fillna(9999).clip(0,9999)

features['propensity'] = model.predict_proba(X)[:,1]
features['decile'] = (features['propensity'].rank(pct=True)*10).astype(int).clip(1,10)

out = ENT_ROOT/'data/processed/scored_donors.csv'
out.parent.mkdir(parents=True, exist_ok=True)
features.sort_values('propensity', ascending=False).to_csv(out, index=False)
print('Scored donors →', out)
