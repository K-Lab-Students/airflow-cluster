import mlflow
import mlflow.sklearn
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score
import argparse
import sys
import subprocess
import datetime
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description="Расширенный MLflow Скрипт для Логирования Метрик и Модели на Каждой Итерации")
    parser.add_argument(
        "--experiment_name",
        type=str,
        default=f"Iris_SGDClassifier_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
        help="Название MLflow эксперимента."
    )
    parser.add_argument(
        "--tracking_uri",
        type=str,
        default="http://192.168.0.70:5000/",
        help="URI MLflow tracking сервера."
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=100,
        help="Количество эпох обучения."
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=32,
        help="Размер батча для обучения."
    )
    return parser.parse_args()

def log_environment():
    # Логирование версии Python
    mlflow.log_param("python_version", sys.version)

    # Логирование установленных пакетов
    try:
        installed_packages = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze']).decode('utf-8')
        mlflow.log_text(installed_packages, "requirements.txt")
    except Exception as e:
        mlflow.log_param("error_logging_packages", str(e))

def main():
    args = parse_args()
    
    # Установка URI для MLflow tracking сервера
    mlflow.set_tracking_uri(args.tracking_uri)
    
    # Установка названия эксперимента
    experiment_name = args.experiment_name
    mlflow.set_experiment(experiment_name)
    
    # Загрузка датасета Iris
    iris = datasets.load_iris()
    X = iris.data
    y = iris.target

    # Разделение данных на обучающую и тестовую выборки
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Определение уникальных классов для partial_fit
    classes = np.unique(y_train)

    # Начало MLflow run
    try:
        with mlflow.start_run():
            # Логирование деталей окружения
            log_environment()

            # Логирование гиперпараметров
            learning_rate = 0.01
            max_epochs = args.epochs
            batch_size = args.batch_size
            loss = 'log'  # Логистическая регрессия
            penalty = 'l2'
            mlflow.log_param("learning_rate", learning_rate)
            mlflow.log_param("max_epochs", max_epochs)
            mlflow.log_param("batch_size", batch_size)
            mlflow.log_param("loss", loss)
            mlflow.log_param("penalty", penalty)

            # Инициализация SGDClassifier
            model = SGDClassifier(
                loss=loss,
                penalty=penalty,
                learning_rate='constant',
                eta0=learning_rate,
                max_iter=1,  # Будем контролировать количество эпох вручную
                warm_start=True,
                random_state=42
            )

            # Обучение модели по эпохам
            for epoch in range(1, max_epochs + 1):
                # Перемешивание данных
                indices = np.arange(X_train.shape[0])
                np.random.shuffle(indices)
                X_train_shuffled = X_train[indices]
                y_train_shuffled = y_train[indices]

                # Обучение модели на всех данных по батчам
                for start in range(0, X_train.shape[0], batch_size):
                    end = start + batch_size
                    X_batch = X_train_shuffled[start:end]
                    y_batch = y_train_shuffled[start:end]
                    model.partial_fit(X_batch, y_batch, classes=classes)

                # Предсказания на тестовой выборке
                y_pred = model.predict(X_test)

                # Вычисление метрик
                accuracy = accuracy_score(y_test, y_pred)
                precision = precision_score(y_test, y_pred, average='weighted')
                recall = recall_score(y_test, y_pred, average='weighted')

                # Логирование метрик на текущей эпохе
                mlflow.log_metric("accuracy", accuracy, step=epoch)
                mlflow.log_metric("precision", precision, step=epoch)
                mlflow.log_metric("recall", recall, step=epoch)

                # Вывод метрик в консоль
                print(f"Эпоха {epoch}/{max_epochs} - Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}")

            # Логирование окончательной модели
            mlflow.sklearn.log_model(model, "model")

            # Вывод информации о запуске
            run_id = mlflow.active_run().info.run_id
            experiment_id = mlflow.active_run().info.experiment_id
            print(f"Run ID: {run_id}")
            print(f"Experiment ID: {experiment_id}")
            print(f"Метрики и модель сохранены на MLflow сервере по адресу {args.tracking_uri}")

    except Exception as e:
        # Логирование ошибки
        mlflow.log_param("training_error", str(e))
        print(f"Произошла ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
