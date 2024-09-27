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

# Параметры общего хранилища (например, PersistentVolumeClaim)
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
    description='DAG для обучения модели на Yelp Reviews с расширенным логированием',
) as dag:

    prepare_dataset = KubernetesPodOperator(
        task_id='prepare_dataset',
        name='prepare-dataset',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            set -e  # Останавливает выполнение при ошибке
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
            "request_cpu": "2",
            "request_memory": "4Gi",
            "limit_cpu": "4",
            "limit_memory": "8Gi",
        },
        execution_timeout=timedelta(minutes=30),
        get_logs=True,  # Убедитесь, что логи собираются
    )

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
            "request_cpu": "4",
            "request_memory": "8Gi",
            "limit_cpu": "8",
            "limit_memory": "16Gi",
        },
        execution_timeout=timedelta(minutes=60),
        get_logs=True,
    )

    create_subsets = KubernetesPodOperator(
        task_id='create_subsets',
        name='create-subsets',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            """
            set -e
            echo "Starting subset creation..."
            pip install --no-cache-dir datasets
            echo "Datasets library installed successfully."
            python -c "
import os
from datasets import load_from_disk

try:
    print('Loading tokenized dataset...')
    tokenized_datasets = load_from_disk('/shared-data/tokenized_dataset')
    print('Tokenized dataset loaded successfully.')

    print('Creating small train and eval subsets...')
    small_train_dataset = tokenized_datasets['train'].shuffle(seed=42).select(range(1000))
    small_eval_dataset = tokenized_datasets['test'].shuffle(seed=42).select(range(1000))
    print('Subsets created successfully.')

    print('Saving subsets...')
    small_train_dataset.save_to_disk('/shared-data/small_train_dataset')
    small_eval_dataset.save_to_disk('/shared-data/small_eval_dataset')
    print('Subsets saved successfully.')
except Exception as e:
    print(f'Error during subset creation: {e}')
    exit(1)
            "
            echo "Subset creation completed."
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        resources={
            "request_cpu": "2",
            "request_memory": "4Gi",
            "limit_cpu": "4",
            "limit_memory": "8Gi",
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
            """
            set -e
            echo "Starting model training..."
            pip install --no-cache-dir datasets transformers torch evaluate
            echo "Datasets, Transformers, Torch, and Evaluate libraries installed successfully."
            python -c "
import os
from datasets import load_from_disk
from transformers import AutoModelForSequenceClassification, TrainingArguments, Trainer
import evaluate
import numpy as np

try:
    print('Loading training and evaluation datasets...')
    train_dataset = load_from_disk('/shared-data/small_train_dataset')
    eval_dataset = load_from_disk('/shared-data/small_eval_dataset')
    print('Datasets loaded successfully.')

    print('Loading pretrained model...')
    model = AutoModelForSequenceClassification.from_pretrained('google-bert/bert-base-cased', num_labels=5)
    print('Model loaded successfully.')

    print('Setting up training arguments...')
    training_args = TrainingArguments(
        output_dir='/shared-data/test_trainer',
        evaluation_strategy='epoch',
        per_device_train_batch_size=8,
        per_device_eval_batch_size=8,
        num_train_epochs=3,
        weight_decay=0.01,
    )
    print('Training arguments set successfully.')

    print('Loading evaluation metric...')
    metric = evaluate.load('accuracy')
    print('Evaluation metric loaded successfully.')

    def compute_metrics(eval_pred):
        logits, labels = eval_pred
        predictions = np.argmax(logits, axis=-1)
        return metric.compute(predictions=predictions, references=labels)

    print('Initializing Trainer...')
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=compute_metrics,
    )
    print('Trainer initialized successfully.')

    print('Starting training...')
    trainer.train()
    print('Training completed successfully.')

    print('Saving fine-tuned model...')
    trainer.save_model('/shared-data/fine_tuned_model')
    print('Model saved successfully.')

    print('Starting evaluation...')
    results = trainer.evaluate()
    print('Evaluation Results:', results)
except Exception as e:
    print(f'Error during model training: {e}')
    exit(1)
            "
            echo "Model training completed."
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        resources={
            "request_cpu": "16",
            "request_memory": "32Gi",
            "limit_cpu": "32",
            "limit_memory": "64Gi",
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
            """
            set -e
            echo "Starting model evaluation..."
            pip install --no-cache-dir transformers datasets torch evaluate
            echo "Transformers, Datasets, Torch, and Evaluate libraries installed successfully."
            python -c "
import os
from transformers import AutoModelForSequenceClassification, Trainer
from datasets import load_from_disk
import evaluate
import numpy as np

try:
    print('Loading fine-tuned model and evaluation dataset...')
    model = AutoModelForSequenceClassification.from_pretrained('/shared-data/fine_tuned_model')
    eval_dataset = load_from_disk('/shared-data/small_eval_dataset')
    print('Model and dataset loaded successfully.')

    print('Setting up Trainer for evaluation...')
    trainer = Trainer(
        model=model,
        eval_dataset=eval_dataset,
        compute_metrics=lambda eval_pred: {
            'accuracy': evaluate.load('accuracy').compute(
                predictions=np.argmax(eval_pred[0], axis=-1),
                references=eval_pred[1]
            )['accuracy']
        },
    )
    print('Trainer set up successfully.')

    print('Starting evaluation...')
    results = trainer.evaluate()
    print('Final Evaluation Results:', results)
except Exception as e:
    print(f'Error during model evaluation: {e}')
    exit(1)
            "
            echo "Model evaluation completed."
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        resources={
            "request_cpu": "8",
            "request_memory": "16Gi",
            "limit_cpu": "16",
            "limit_memory": "32Gi",
            "nvidia.com/gpu": "1",
        },
        execution_timeout=timedelta(hours=1),
        get_logs=True,
    )

    # Опциональный шаг: Native PyTorch Training Loop
    native_pytorch_training = KubernetesPodOperator(
        task_id='native_pytorch_training',
        name='native-pytorch-training',
        namespace='default',
        image='python:3.9-slim',
        cmds=["bash", "-c"],
        arguments=[
            """
            set -e
            echo "Starting native PyTorch training loop..."
            pip install --no-cache-dir transformers datasets torch evaluate tqdm
            echo "Transformers, Datasets, Torch, Evaluate, and TQDM libraries installed successfully."
            python -c "
import os
from datasets import load_from_disk
from torch.utils.data import DataLoader
from transformers import AutoModelForSequenceClassification
from torch.optim import AdamW
from transformers import get_scheduler
import torch
from tqdm.auto import tqdm
import evaluate
import numpy as np

try:
    print('Loading training and evaluation datasets...')
    train_dataset = load_from_disk('/shared-data/small_train_dataset')
    eval_dataset = load_from_disk('/shared-data/small_eval_dataset')
    print('Datasets loaded successfully.')

    print('Creating DataLoaders...')
    train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=8)
    eval_dataloader = DataLoader(eval_dataset, batch_size=8)
    print('DataLoaders created successfully.')

    print('Loading pretrained model...')
    model = AutoModelForSequenceClassification.from_pretrained('google-bert/bert-base-cased', num_labels=5)
    print('Model loaded successfully.')

    print('Setting device...')
    device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
    model.to(device)
    print(f'Using device: {device}')

    print('Setting up optimizer and scheduler...')
    optimizer = AdamW(model.parameters(), lr=5e-5)
    num_epochs = 3
    num_training_steps = num_epochs * len(train_dataloader)
    lr_scheduler = get_scheduler(
        name='linear', optimizer=optimizer, num_warmup_steps=0, num_training_steps=num_training_steps
    )
    print('Optimizer and scheduler set up successfully.')

    print('Loading evaluation metric...')
    metric = evaluate.load('accuracy')
    print('Evaluation metric loaded successfully.')

    print('Starting training loop...')
    progress_bar = tqdm(range(num_training_steps))
    model.train()
    for epoch in range(num_epochs):
        print(f'Starting epoch {epoch + 1}/{num_epochs}...')
        for batch in train_dataloader:
            batch = {k: v.to(device) for k, v in batch.items()}
            outputs = model(**batch)
            loss = outputs.loss
            loss.backward()

            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()
            progress_bar.update(1)
    print('Training loop completed successfully.')

    print('Starting evaluation loop...')
    model.eval()
    for batch in eval_dataloader:
        batch = {k: v.to(device) for k, v in batch.items()}
        with torch.no_grad():
            outputs = model(**batch)

        logits = outputs.logits
        predictions = torch.argmax(logits, dim=-1)
        metric.add_batch(predictions=predictions, references=batch['labels'])
    results = metric.compute()
    print('Native PyTorch Evaluation Results:', results)
except Exception as e:
    print(f'Error during native PyTorch training: {e}')
    exit(1)
            "
            echo "Native PyTorch training loop completed."
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        resources={
            "request_cpu": "16",
            "request_memory": "32Gi",
            "limit_cpu": "32",
            "limit_memory": "64Gi",
            "nvidia.com/gpu": "1",
        },
        execution_timeout=timedelta(hours=3),
        get_logs=True,
    )

    # Определение зависимостей между задачами
    prepare_dataset >> tokenize_dataset >> create_subsets >> train_model >> evaluate_model
    # Если требуется опциональное обучение с использованием PyTorch
    # evaluate_model >> native_pytorch_training
