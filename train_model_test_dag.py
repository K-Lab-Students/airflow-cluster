from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# Set up logger
logger = logging.getLogger(__name__)

# Function to train a simple model using CPU
def train_model():
    # Load a simple dataset
    data = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(data.data, data.target, test_size=0.2, random_state=42)
    
    # Initialize a simple RandomForestClassifier
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    
    # Train the model
    model.fit(X_train, y_train)
    
    # Predict on the test set
    predictions = model.predict(X_test)
    
    # Calculate accuracy
    accuracy = accuracy_score(y_test, predictions)
    
    # Log the results
    logger.info(f"Model trained with accuracy: {accuracy}")
    print(f"Model trained with accuracy: {accuracy}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='train_model_on_cpu',
    default_args=default_args,
    description='A DAG to train a simple model on CPU',
    schedule_interval=None,  # Executes on trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to install required dependencies
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install scikit-learn',  # Install scikit-learn for training
    )

    # Task to train the model
    train_model_task = PythonOperator(
        task_id='train_model_task',
        python_callable=train_model,
    )

    # Define task execution order
    install_dependencies >> train_model_task
