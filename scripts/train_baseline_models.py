from src.model_input import select_model_features
from src.data_io.fetch import fetch_raw_data
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score
from imblearn.over_sampling import SMOTE
import numpy as np
from src.model_input import check_nan

TABLE_NAME = "LOAN_DATA_CLEAN"

def scale_data(X_train, X_test):
    scaler = StandardScaler()
    return scaler.fit_transform(X_train), scaler.transform(X_test)

def apply_smote(X_train, y_train):
    smote = SMOTE(random_state=42)
    return smote.fit_resample(X_train, y_train)

def evaluate_model(model, X_test, y_test, threshold=0.5):
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= threshold).astype(int)
    
    print(classification_report(y_test, y_pred))
    print("ROC AUC Score:", round(roc_auc_score(y_test, y_proba), 4))

def train_and_evaluate(models, X_train, X_test, y_train, y_test, scale_for=None, use_smote=False, threshold=0.5):
    if scale_for:
        X_train, X_test = scale_data(X_train, X_test)

    if use_smote:
        X_train, y_train = apply_smote(X_train, y_train)

    for name, model in models.items():
        print(f"\n=== {name} ===")
        model.fit(X_train, y_train)
        evaluate_model(model, X_test, y_test, threshold=threshold)

if __name__ == "__main__":
    df_clean = fetch_raw_data(TABLE_NAME)
    X, y = select_model_features(df_clean)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, stratify=y, test_size=0.2, random_state=42
    )

    check_nan(X_train)

    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42),
        "Random Forest": RandomForestClassifier(random_state=42, class_weight="balanced"),
        "XGBoost": XGBClassifier(use_label_encoder=False, eval_metric='logloss', scale_pos_weight=(y_train == 0).sum() / (y_train == 1).sum())
    }

    train_and_evaluate(
        models=models,
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        scale_for="Logistic Regression",  # Only LogisticRegression needs scaling
        use_smote=True,
        threshold=0.35  # Experiment with this for recall/precision tradeoff
    )
