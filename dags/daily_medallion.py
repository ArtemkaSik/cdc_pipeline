import json
import time
from datetime import datetime
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.task.trigger_rule import TriggerRule
from kubernetes.client import models as k8s


CONNECT_URL = "http://kafka-connect.kafka.svc.cluster.local:8083"
CONNECTOR = "mysql-sink-connector"


def connector_request(action: str) -> None:
    request = Request(
        f"{CONNECT_URL}/connectors/{CONNECTOR}/{action}",
        data=b"",
        method="PUT",
        headers={"Content-Type": "application/json"}
    )
    with urlopen(request, timeout=10):
        pass


def pause_sink() -> None:
    connector_request("pause")

    for _ in range(30):
        req = Request(
            f"{CONNECT_URL}/connectors/{CONNECTOR}/status",
            headers={"Accept": "application/json"}
        )
        with urlopen(f"{CONNECT_URL}/connectors/{CONNECTOR}/status", timeout=10) as response:
            status = json.load(response)
        states = [status["connector"]["state"], *(task["state"] for task in status["tasks"])]
        if states and all(state == "PAUSED" for state in states):
            return
        time.sleep(2)

    raise TimeoutError("Sink-коннектор не перешёл в состояние PAUSED")


with DAG(
        dag_id="daily_medallion",
        schedule="0 2 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["spark", "cdc", "iceberg"],
) as dag:
    pause = PythonOperator(
        task_id="pause_sink",
        python_callable=pause_sink,
    )

    run_spark = KubernetesPodOperator(
        task_id="run_spark_pipeline",
        name="daily-medallion",
        namespace="airflow",
        image="spark-medallion:latest",
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-c"],
        arguments=[
            """
            /opt/spark/bin/spark-submit \
              --master spark://spark-master-svc.spark.svc.cluster.local:7077 \
              --deploy-mode client \
              --conf spark.driver.host=$POD_IP \
              --conf spark.driver.bindAddress=0.0.0.0 \
              --conf spark.executor.instances=1 \
              --conf spark.executor.cores=1 \
              --conf spark.executor.memory=512m \
              local:///opt/spark/app/main.py
            """
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="spark-pipeline")
            )
        ],
        env_vars=[
            k8s.V1EnvVar(
                name="POD_IP",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")
                ),
            )
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "1000m", "memory": "2Gi"},
        ),
        get_logs=True,
        on_finish_action="delete_pod",
	    trigger_rule=TriggerRule.ALL_DONE,
    )

    resume = PythonOperator(
        task_id="resume_sink",
        python_callable=connector_request,
        op_args=["resume"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pause >> run_spark >> resume