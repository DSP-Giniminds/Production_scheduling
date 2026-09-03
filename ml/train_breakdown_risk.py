"""
Trains the breakdown-risk classification model: given a machine's type,
predict the probability that its next run ends in a breakdown.

NOTE ON DATA VOLUME: with ~41 completed runs total and only a subset ending
in breakdown, this is genuinely thin data for a classifier -- expect a rough
starting model, not a reliable one, until more simulated shifts accumulate.
"""

import os

import joblib
import mlflow
import mlflow.xgboost
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from features import build_breakdown_risk_features, fetch_machine_run_transitions

MODEL_OUTPUT_PATH = "models/breakdown_risk_model.joblib"

mlflow.set_experiment("production-scheduling-breakdown-risk")


def main():
    raw = fetch_machine_run_transitions()
    print(f"Loaded {len(raw)} completed machine runs from ClickHouse.")

    breakdown_count = raw["ended_in_breakdown"].sum() if len(raw) else 0
    print(f"{breakdown_count} of these ended in a breakdown.")
    if breakdown_count < 5:
        print("WARNING: very few breakdown examples -- classifier will be unreliable "
              "until more simulated shifts (or real data) accumulate.")

    data = build_breakdown_risk_features(raw)
    X = data.drop(columns=["ended_in_breakdown"])
    y = data["ended_in_breakdown"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.nunique() > 1 else None
    )

    with mlflow.start_run():
        model = XGBClassifier(n_estimators=50, max_depth=3, random_state=42)
        model.fit(X_train, y_train)

        if y_test.nunique() > 1:
            probs = model.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, probs)
            print(f"Test AUC: {auc:.3f} (on {len(X_test)} held-out rows)")
            mlflow.log_metric("test_auc", auc)
        else:
            print("Test set has only one class present -- AUC not meaningful, skipping.")

        mlflow.log_param("n_estimators", 50)
        mlflow.log_param("max_depth", 3)
        mlflow.log_param("training_rows", len(X_train))
        mlflow.log_param("breakdown_count", int(breakdown_count))
        mlflow.xgboost.log_model(model, "model")

        os.makedirs("models", exist_ok=True)
        joblib.dump({"model": model, "feature_columns": list(X.columns)}, MODEL_OUTPUT_PATH)
        print(f"Model saved to {MODEL_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
