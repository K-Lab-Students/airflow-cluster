# 
# 004 Train model on AirFlow with real=time saving metrics and save model in ML Flow
# 

from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def train_model():
    try:
        import mlflow
        import mlflow.sklearn
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score
    except ImportError:
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn", "mlflow"])
        import mlflow
        import mlflow.sklearn
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score

    # Set the MLflow tracking URI
    mlflow.set_tracking_uri("http://192.168.0.70:5000/")

    # Start an MLflow run
    with mlflow.start_run():
        # Load data
        data = load_iris()
        X_train, X_test, y_train, y_test = train_test_split(
            data.data, data.target, test_size=0.2, random_state=42
        )

        # Initialize the model with warm_start=True to add trees incrementally
        model = RandomForestClassifier(
            n_estimators=1,
            warm_start=True,
            random_state=42,
            oob_score=True  # Optional: use out-of-bag estimates
        )

        total_estimators = 10  # Total number of trees
        for i in range(1, total_estimators + 1):
            model.n_estimators = i
            model.fit(X_train, y_train)

            # Make predictions and calculate accuracy
            predictions = model.predict(X_test)
            accuracy = accuracy_score(y_test, predictions)
            print(f"Итерация {i}: Model trained with accuracy: {accuracy}")

            # Log accuracy as a metric
            mlflow.log_metric("accuracy", accuracy, step=i)

            # Log n_estimators as a metric instead of a parameter
            mlflow.log_metric("n_estimators", i, step=i)

        # Log the final model to MLflow
        mlflow.sklearn.log_model(model, "random_forest_model")
