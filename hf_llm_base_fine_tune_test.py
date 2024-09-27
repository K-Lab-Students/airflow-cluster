from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

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
    'hf_transformers_training_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG для обучения модели на Yelp Reviews с использованием Hugging Face и PyTorch',
) as dag:

    prepare_dataset = KubernetesPodOperator(
        task_id='prepare_dataset',
        name='prepare-dataset',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            pip install datasets &&
            python -c "
import os
from datasets import load_dataset

# Загрузка датасета
dataset = load_dataset('yelp_review_full')

# Сохранение датасета в общий volume
dataset.save_to_disk('/shared-data/dataset')
print('Dataset prepared and saved.')
            "
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '4Gi', 'cpu': '2'},
            limits={'memory': '8Gi', 'cpu': '4'}
        ),
        execution_timeout=timedelta(minutes=30),
    )

    tokenize_dataset = KubernetesPodOperator(
        task_id='tokenize_dataset',
        name='tokenize-dataset',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            pip install datasets transformers &&
            python -c "
import os
from datasets import load_from_disk
from transformers import AutoTokenizer

# Загрузка датасета
dataset = load_from_disk('/shared-data/dataset')

# Загрузка токенизатора
tokenizer = AutoTokenizer.from_pretrained('google-bert/bert-base-cased')

# Функция токенизации
def tokenize_function(examples):
    return tokenizer(examples['text'], padding='max_length', truncation=True)

# Применение токенизации
tokenized_datasets = dataset.map(tokenize_function, batched=True)

# Сохранение токенизированного датасета
tokenized_datasets.save_to_disk('/shared-data/tokenized_dataset')
print('Dataset tokenized and saved.')
            "
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '8Gi', 'cpu': '4'},
            limits={'memory': '16Gi', 'cpu': '8'}
        ),
        execution_timeout=timedelta(minutes=60),
    )

    create_subsets = KubernetesPodOperator(
        task_id='create_subsets',
        name='create-subsets',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            pip install datasets &&
            python -c "
import os
from datasets import load_from_disk

# Загрузка токенизированного датасета
tokenized_datasets = load_from_disk('/shared-data/tokenized_dataset')

# Создание подмножеств
small_train_dataset = tokenized_datasets['train'].shuffle(seed=42).select(range(1000))
small_eval_dataset = tokenized_datasets['test'].shuffle(seed=42).select(range(1000))

# Сохранение подмножеств
small_train_dataset.save_to_disk('/shared-data/small_train_dataset')
small_eval_dataset.save_to_disk('/shared-data/small_eval_dataset')
print('Subsets created and saved.')
            "
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'cpu': 'true'},
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '4Gi', 'cpu': '2'},
            limits={'memory': '8Gi', 'cpu': '4'}
        ),
        execution_timeout=timedelta(minutes=30),
    )

    train_model = KubernetesPodOperator(
        task_id='train_model',
        name='train-model',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            pip install datasets transformers torch evaluate &&
            python -c "
import os
from datasets import load_from_disk
from transformers import AutoModelForSequenceClassification, TrainingArguments, Trainer
import evaluate
import numpy as np

# Загрузка подмножеств
train_dataset = load_from_disk('/shared-data/small_train_dataset')
eval_dataset = load_from_disk('/shared-data/small_eval_dataset')

# Загрузка модели
model = AutoModelForSequenceClassification.from_pretrained('google-bert/bert-base-cased', num_labels=5)

# Настройка аргументов обучения
training_args = TrainingArguments(
    output_dir='/shared-data/test_trainer',
    evaluation_strategy='epoch',
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    num_train_epochs=3,
    weight_decay=0.01,
)

# Загрузка метрики
metric = evaluate.load('accuracy')

# Функция вычисления метрик
def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    return metric.compute(predictions=predictions, references=labels)

# Инициализация Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    compute_metrics=compute_metrics,
)

# Обучение модели
trainer.train()

# Сохранение модели
trainer.save_model('/shared-data/fine_tuned_model')

# Оценка модели
results = trainer.evaluate()
print('Evaluation Results:', results)
            "
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '32Gi', 'cpu': '16', 'nvidia.com/gpu': '1'},
            limits={'memory': '64Gi', 'cpu': '32', 'nvidia.com/gpu': '1'}
        ),
        execution_timeout=timedelta(hours=2),
    )

    evaluate_model = KubernetesPodOperator(
        task_id='evaluate_model',
        name='evaluate-model',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            pip install transformers datasets torch evaluate &&
            python -c "
import os
from transformers import AutoModelForSequenceClassification, Trainer
from datasets import load_from_disk
import evaluate
import numpy as np

# Загрузка модели и подмножеств
model = AutoModelForSequenceClassification.from_pretrained('/shared-data/fine_tuned_model')
eval_dataset = load_from_disk('/shared-data/small_eval_dataset')

# Настройка Trainer
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

# Оценка модели
results = trainer.evaluate()
print('Final Evaluation Results:', results)
            "
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '16Gi', 'cpu': '8', 'nvidia.com/gpu': '1'},
            limits={'memory': '32Gi', 'cpu': '16', 'nvidia.com/gpu': '1'}
        ),
        execution_timeout=timedelta(hours=1),
    )

    # Опциональный шаг: Native PyTorch Training Loop
    native_pytorch_training = KubernetesPodOperator(
        task_id='native_pytorch_training',
        name='native-pytorch-training',
        namespace='default',
        image='python:3.9-slim',  # Используем базовый Python-образ
        cmds=["bash", "-c"],
        arguments=[
            """
            pip install transformers datasets torch evaluate tqdm &&
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

# Загрузка подмножеств
train_dataset = load_from_disk('/shared-data/small_train_dataset')
eval_dataset = load_from_disk('/shared-data/small_eval_dataset')

# Создание DataLoader
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=8)
eval_dataloader = DataLoader(eval_dataset, batch_size=8)

# Загрузка модели
model = AutoModelForSequenceClassification.from_pretrained('google-bert/bert-base-cased', num_labels=5)

# Настройка устройства
device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
model.to(device)

# Настройка оптимизатора и планировщика
optimizer = AdamW(model.parameters(), lr=5e-5)
num_epochs = 3
num_training_steps = num_epochs * len(train_dataloader)
lr_scheduler = get_scheduler(
    name='linear', optimizer=optimizer, num_warmup_steps=0, num_training_steps=num_training_steps
)

# Настройка метрики
metric = evaluate.load('accuracy')

# Цикл обучения
progress_bar = tqdm(range(num_training_steps))
model.train()
for epoch in range(num_epochs):
    for batch in train_dataloader:
        batch = {k: v.to(device) for k, v in batch.items()}
        outputs = model(**batch)
        loss = outputs.loss
        loss.backward()

        optimizer.step()
        lr_scheduler.step()
        optimizer.zero_grad()
        progress_bar.update(1)

# Цикл оценки
model.eval()
for batch in eval_dataloader:
    batch = {k: v.to(device) for k, v in batch.items()}
    with torch.no_grad():
        outputs = model(**batch)

    logits = outputs.logits
    predictions = torch.argmax(logits, dim=-1)
    metric.add_batch(predictions=predictions, references=batch['labels'])

# Вывод результатов оценки
results = metric.compute()
print('Native PyTorch Evaluation Results:', results)
            "
            """
        ],
        volumes=[shared_volume],
        volume_mounts=[volume_mount],
        node_selector={'gpu': 'true'},
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '32Gi', 'cpu': '16', 'nvidia.com/gpu': '1'},
            limits={'memory': '64Gi', 'cpu': '32', 'nvidia.com/gpu': '1'}
        ),
        execution_timeout=timedelta(hours=3),
    )

    # Определение зависимостей между задачами
    prepare_dataset >> tokenize_dataset >> create_subsets >> train_model >> evaluate_model
    # Если требуется опциональное обучение с использованием PyTorch
    # evaluate_model >> native_pytorch_training
