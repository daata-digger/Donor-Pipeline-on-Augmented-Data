import pandas as pd, numpy as np, pathlib, joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from xgboost import XGBClassifier

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
features = pd.read_csv(PROJECT_ROOT/'data/processed/curated/donor_features.csv')

y = ((features['recency_days'] < 120) |
     (features['frequency'] >= 3) |
     (features['total_amount'] >= features['total_amount'].median())).astype(int)

X = features[['frequency','total_amount','recency_days','events_attended','volunteer_hours','wealth_score_ext']].copy()
X['recency_days'] = X['recency_days'].fillna(9999).clip(0, 9999)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

model = XGBClassifier(n_estimators=400, max_depth=4, learning_rate=0.07,
                      subsample=0.9, colsample_bytree=0.9, n_jobs=-1,
                      eval_metric='logloss', random_state=42)
model.fit(X_train, y_train)

proba = model.predict_proba(X_test)[:,1]
auc = roc_auc_score(y_test, proba)
print(f"AUC: {auc:.3f}")
print(classification_report(y_test, (proba>0.5).astype(int)))

(PROJECT_ROOT/'ml').mkdir(exist_ok=True, parents=True)
joblib.dump(model, PROJECT_ROOT/'ml/model_xgb.pkl')
print('Saved model → ml/model_xgb.pkl')
