# Финалный проект

Pipeline: CDC из PostgreSQL в Kafka, bronze-слой в MySQL, раз в сутки Spark сверяет source и bronze, пишет silver/gold в MinIO (Iceberg) и делает compaction.

```text
PostgreSQL (source)
    -> Debezium -> Kafka -> JDBC Sink -> MySQL (bronze, append-only журнал CDC)
                                              |
                                         Spark (раз в сутки, Airflow)
                                              |
                              MinIO / Iceberg: silver + gold
```

Bronze хранит все CDC-события с техническими полями (`__op`, `__deleted`, `__source_ts_ms`, `__source_lsn`). Актуальный срез для сверки — последняя запись по ключу; если `__deleted=true`, ключ не считается.

Silver — те же данные без технических полей. Gold — простые агрегаты (`client_turnover`, `operation_summary`).

---

## Kubernetes (запуск кластера)

Старт minikube с монтированием папки DAG-ов (Windows-путь подставь свой):

```bash
minikube start --mount --mount-string="C:\Users\SikachevAN\OpenideProjects\sikachev_an\dags:/mnt/airflow-dags" --cpus=4 --memory=7783 --driver=docker
```

Если mount при старте не сработал, можно отдельно:

```bash
minikube mount C:\Users\SikachevAN\OpenideProjects\sikachev_an\dags:/mnt/airflow-dags
```

Ingress:

```bash
minikube addons enable ingress
kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller
```

---

## Секреты

Перед деплоем нужно подготовить секреты (файлы с паролями в git не коммитятся).

**Airflow** — `k8s/airflow/secrets.yaml` (логин UI, api-secret-key и т.д.).

Ключ для API:

```bash
python3 -c "import secrets; print(secrets.token_hex(16))"
```

Вставить в `airflow-api-secret`.

**Spark** — скопировать пример и подставить пароли:

```bash
cp k8s/airflow/spark-secrets.example.yaml k8s/airflow/spark-secrets.yaml
```

Логин/пароль MinIO в `spark-secrets.yaml` должны совпадать с `k8s/s3/secrets.yaml`.

**Базы** — `k8s/databases/postgres-secrets.yaml`, `k8s/databases/mysql-secrets.yaml` (создать по аналогии с тем, что ожидают манифесты postgres/mysql).

**MinIO** — `k8s/s3/secrets.yaml`.

---

## Порядок развёртывания

Helm-репозитории один раз:

```bash
helm repo add airflow https://airflow.apache.org
helm repo add minio https://charts.min.io/
helm repo update
```

### 1. Базы данных

Источник — PostgreSQL. Bronze — MySQL (не Oracle).

```bash
kubectl apply -f k8s/databases/namespace.yaml
kubectl apply -f k8s/databases/
```

Port-forward для DBeaver и т.п.:

```bash
kubectl port-forward svc/mysql 3306:3306 -n databases
kubectl port-forward svc/postgresql 5432:5432 -n databases
```

### 2. Kafka

```bash
kubectl apply -f k8s/kafka/namespace.yaml
kubectl apply -f k8s/kafka/
```

Kafka UI (Schema Registry уже прописан в конфиге):

```bash
kubectl port-forward svc/kafka-ui 8080:8080 -n kafka
```

NodePort, если удобнее: `http://<minikube-ip>:30080`.

#### CDC-коннекторы

Сначала port-forward на Connect:

```bash
kubectl port-forward svc/kafka-connect 8083:8083 -n kafka
```

Из корня проекта (PowerShell):

```powershell
curl.exe -X POST -H "Content-Type: application/json" --data @k8s/kafka/kafka_connectors/postgres-source-connector.json http://localhost:8083/connectors
curl.exe -X POST -H "Content-Type: application/json" --data @k8s/kafka/kafka_connectors/mysql-sink-connector.json http://localhost:8083/connectors
```

Source — Debezium PostgreSQL. Sink — JDBC в MySQL, режим insert (bronze как журнал событий).

### 3. MinIO

```bash
kubectl apply -f k8s/s3/namespace.yaml
kubectl apply -f k8s/s3/secrets.yaml
helm upgrade --install minio minio/minio --version 5.4.0 -n s3 -f k8s/s3/minio-values.yaml
```

Консоль MinIO (бакет `lakehouse`, внутри Iceberg warehouse `silver` и `gold`):

```bash
kubectl port-forward svc/minio-console 9001:9001 -n s3
```

`--version` для helm лучше указывать явно — после `helm repo update` без версии может приехать другой чарт.

### 4. Spark

Образ собирается внутри minikube (после каждого изменения `main.py` — пересобрать):

```bash
minikube image build -t spark-medallion:latest k8s/spark
kubectl apply -f k8s/spark/namespace.yaml
kubectl apply -f k8s/spark/spark.yaml
kubectl -n spark rollout status deployment/spark-master
kubectl -n spark rollout status deployment/spark-worker
```

Spark UI:

```bash
kubectl -n spark port-forward svc/spark-master 8081:8080
```

Проверить, что в образ попал новый код:

```bash
minikube ssh -- docker run --rm spark-medallion:latest head -5 /opt/spark/app/main.py
```

### 5. Airflow

```bash
kubectl apply -f k8s/airflow/namespace.yaml
kubectl apply -f k8s/airflow/secrets.yaml
kubectl apply -f k8s/airflow/spark-secrets.yaml

helm upgrade --install airflow airflow/airflow --version 1.22.0 -n airflow -f k8s/airflow/airflow-values.yaml

kubectl apply -f k8s/airflow/spark-rbac.yaml
```

UI — через ingress `airflow.local` или по-старинке:

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

DAG-и подхватываются из `/mnt/airflow-dags` (папка `dags` в репозитории).

---

## DAG-и

| DAG | Назначение |
|-----|------------|
| `simulate_*_activity` | Генерация INSERT/UPDATE/DELETE в PostgreSQL для проверки CDC |
| `daily_medallion` | Ежедневный batch: pause sink → Spark → resume sink |

### daily_medallion (расписание 02:00)

1. Ставит на паузу `mysql-sink-connector`.
2. Сверяет ключи PostgreSQL и актуального среза bronze в MySQL.
3. При расхождениях дописывает repair/delete-события в bronze.
4. Пишет silver (без технических полей) и gold (агрегаты) в Iceberg.
5. Compaction через `rewrite_data_files`.
6. Возобновляет sink (даже если Spark упал).

Compaction отработал, если в metadata таблицы есть `v2.metadata.json` и во втором snapshot `"operation": "replace"`. Первый snapshot после записи — `"overwrite"`. Старые parquet в MinIO после compaction могут ещё лежать на диске — Iceberg на них уже не смотрит, это нормально.

---