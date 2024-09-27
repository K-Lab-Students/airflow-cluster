from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
import os

from kubernetes.client import models as k8s

default_args = {
    'start_date': datetime(2024, 4, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Parameters for shared storage (e.g., PersistentVolumeClaim)
shared_volume = k8s.V1Volume(
    name='shared-data',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='shared-pvc'
    )
)

volume_mount = k8s.V1VolumeMount(
    name='shared-data',
    mount_path='/shared-data'
)

with DAG(
    'hf_transformers_training_dag_with_logging',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG for training a model on Yelp Reviews with enhanced logging',
) as dag:

    prepare_dataset = KubernetesPodOperator(
        task_id='prepare_dataset',
        name='prepare-dataset',
        namespace='default',
        image='python:3.9-slim',  # Using a base Python image
        cmds=["bash", "-c"],
        arguments=[
            """
            set -e  # Stops execution on error
            echo "Starting dataset preparation..."
            pip install --no-cache-dir datasets
            echo "Datasets library installed successfully."
            python -c "
import os
from datasets import load_dataset

try:
    print('Loading Yelp Reviews dataset...')
    dataset = load_dataset('yelp_review_full')
    print('Dataset loaded successfully.')
    print('Saving dataset to /shared-data/dataset...')
    dataset.save_to_disk('/shared-data/dataset')
    print('Dataset saved successfully.')
except Exception as e:
    print(f'Error during dataset preparation: {e}')
    exit(1)
            "
            echo "Dataset preparation completed."
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        resources={
            "requests_cpu": "2",
            "requests_memory": "4Gi",
            "limits_cpu": "4",
            "limits_memory": "8Gi",
        },
        execution_timeout=timedelta(minutes=30),
        get_logs=True,  # Ensure logs are collected
    )

    # Repeat similar changes for other KubernetesPodOperator instances
    tokenize_dataset = KubernetesPodOperator(
        task_id='tokenize_dataset',
        name='tokenize-dataset',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            """
            set -e
            echo "Starting dataset tokenization..."
            pip install --no-cache-dir datasets transformers
            echo "Datasets and Transformers libraries installed successfully."
            python -c "
import os
from datasets import load_from_disk
from transformers import AutoTokenizer

try:
    print('Loading prepared dataset...')
    dataset = load_from_disk('/shared-data/dataset')
    print('Dataset loaded successfully.')

    print('Loading tokenizer...')
    tokenizer = AutoTokenizer.from_pretrained('google-bert/bert-base-cased')
    print('Tokenizer loaded successfully.')

    def tokenize_function(examples):
        return tokenizer(examples['text'], padding='max_length', truncation=True)

    print('Tokenizing dataset...')
    tokenized_datasets = dataset.map(tokenize_function, batched=True)
    print('Dataset tokenized successfully.')

    print('Saving tokenized dataset...')
    tokenized_datasets.save_to_disk('/shared-data/tokenized_dataset')
    print('Tokenized dataset saved successfully.')
except Exception as e:
    print(f'Error during tokenization: {e}')
    exit(1)
            "
            echo "Dataset tokenization completed."
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        resources={
            "requests_cpu": "4",
            "requests_memory": "8Gi",
            "limits_cpu": "8",
            "limits_memory": "16Gi",
        },
        execution_timeout=timedelta(minutes=60),
        get_logs=True,
    )

    # Continue updating other tasks similarly...
    # For example:

    create_subsets = KubernetesPodOperator(
        task_id='create_subsets',
        name='create-subsets',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            # ... your script ...
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        resources={
            "requests_cpu": "2",
            "requests_memory": "4Gi",
            "limits_cpu": "4",
            "limits_memory": "8Gi",
        },
        execution_timeout=timedelta(minutes=30),
        get_logs=True,
    )

    train_model = KubernetesPodOperator(
        task_id='train_model',
        name='train-model',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            # ... your script ...
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        resources={
            "requests_cpu": "16",
            "requests_memory": "32Gi",
            "limits_cpu": "32",
            "limits_memory": "64Gi",
            "nvidia.com/gpu": "1",
        },
        execution_timeout=timedelta(hours=2),
        get_logs=True,
    )

    evaluate_model = KubernetesPodOperator(
        task_id='evaluate_model',
        name='evaluate-model',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            # ... your script ...
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        resources={
            "requests_cpu": "8",
            "requests_memory": "16Gi",
            "limits_cpu": "16",
            "limits_memory": "32Gi",
            "nvidia.com/gpu": "1",
        },
        execution_timeout=timedelta(hours=1),
        get_logs=True,
    )

    # Optional step: Native PyTorch Training Loop
    native_pytorch_training = KubernetesPodOperator(
        task_id='native_pytorch_training',
        name='native-pytorch-training',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            # ... your script ...
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        resources={
            "requests_cpu": "16",
            "requests_memory": "32Gi",
            "limits_cpu": "32",
            "limits_memory": "64Gi",
            "nvidia.com/gpu": "1",
        },
        execution_timeout=timedelta(hours=3),
        get_logs=True,
    )

    # Define task dependencies
    prepare_dataset >> tokenize_dataset >> create_subsets >> train_model >> evaluate_model
    # If optional PyTorch training is needed:
    # evaluate_model >> native_pytorch_training
