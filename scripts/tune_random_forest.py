from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler
from imblearn.pipeline import Pipeline
from imblearn.over_sampling import SMOTE
from scipy.stats import randint
import pandas as pd
import numpy as np

from src.data_io.fetch import fetch_raw_data
from src.model_input import select_model_features

# Load and split data
df_clean = fetch_raw_data("LOAN_DATA_CLEAN")
X, y = select_model_features(df_clean)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, stratify=y, test_size=0.2, random_state=42
)

# Pipeline: Scaling -> SMOTE -> Random Forest
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('smote', SMOTE(random_state=42)),
    ('rf', RandomForestClassifier(random_state=42))
])

# Hyperparameter search space
param_dist = {
    'rf__n_estimators': randint(100, 500),
    'rf__max_depth': randint(5, 30),
    'rf__min_samples_split': randint(2, 10),
    'rf__min_samples_leaf': randint(1, 5),
    'rf__max_features': ['sqrt', 'log2'],
    'rf__class_weight': ['balanced']
}

# Random search with 5-fold CV
search = RandomizedSearchCV(
    estimator=pipeline,
    param_distributions=param_dist,
    n_iter=20,
    scoring='roc_auc',
    cv=5,
    verbose=2,
    random_state=42,
    n_jobs=-1
)

search.fit(X_train, y_train)

# Best model
best_model = search.best_estimator_

# Predict on test set
X_test_scaled = best_model.named_steps['scaler'].transform(X_test)
y_proba = best_model.named_steps['rf'].predict_proba(X_test_scaled)[:, 1]
y_pred = best_model.named_steps['rf'].predict(X_test_scaled)

print("\nBest Parameters:\n", search.best_params_)
print("\nROC AUC on Test Set:", round(roc_auc_score(y_test, y_proba), 4))
print("\nClassification Report:\n", classification_report(y_test, y_pred))
