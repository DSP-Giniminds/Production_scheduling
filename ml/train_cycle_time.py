"""
Trains the cycle-time regression model: given a machine's type, predict how
long a run will take (in minutes).

NOTE ON DATA VOLUME: as of this build, ClickHouse has ~41 completed machine
runs from a single simulated shift. That's enough to validate the pipeline
end-to-end, but not enough for a genuinely reliable model -- treat this run
as "does the pipeline work," and retrain once more simulated shifts (or real
data) have accumulated.
"""

import os

import joblib
import mlflow
import mlflow.xgboost
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from features import build_cycle_time_features, fetch_machine_run_transitions

MODEL_OUTPUT_PATH = "models/cycle_time_model.joblib"

mlflow.set_experiment("production-scheduling-cycle-time")


def main():
    raw = fetch_machine_run_transitions()
    print(f"Loaded {len(raw)} completed machine runs from ClickHouse.")

    if len(raw) < 10:
        print("WARNING: very little training data -- model quality will be poor. "
              "Consider running the generator for more simulated volume first.")

    data = build_cycle_time_features(raw)
    X = data.drop(columns=["duration_minutes"])
    y = data["duration_minutes"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    with mlflow.start_run():
        model = XGBRegressor(n_estimators=50, max_depth=3, random_state=42)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae = mean_absolute_error(y_test, preds)
        print(f"Test MAE: {mae:.2f} minutes (on {len(X_test)} held-out rows)")

        mlflow.log_param("n_estimators", 50)
        mlflow.log_param("max_depth", 3)
        mlflow.log_param("training_rows", len(X_train))
        mlflow.log_metric("test_mae_minutes", mae)
        mlflow.xgboost.log_model(model, "model")

        os.makedirs("models", exist_ok=True)
        joblib.dump({"model": model, "feature_columns": list(X.columns)}, MODEL_OUTPUT_PATH)
        print(f"Model saved to {MODEL_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
