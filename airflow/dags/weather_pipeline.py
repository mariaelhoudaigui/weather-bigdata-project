from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import subprocess


def start_nifi_flow():
    processor_id = "0b1024d0-019a-1000-df8a-68baa1426991"  # <-- à remplacer
    url = f"http://nifi:8080/nifi-api/processors/{processor_id}/run-status"
    response = requests.put(url, json={"state": "RUNNING"})
    if response.status_code == 200:
        print("NiFi flow started successfully")
    else:
        print("Failed to start NiFi flow:", response.text)
        response.raise_for_status()
def run_spark_job():
    cmd = (
        "/spark/bin/spark-submit "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        "/app/weather_streaming.py"
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("Spark job completed successfully")
    else:
        print("Spark job failed:", result.stderr)
        raise Exception("Spark job failed")




# -------- Airflow DAG -------- #

default_args = {
    "owner": "Maria",
    "start_date": days_ago(1)
}

with DAG(
    dag_id="bigdata_pipeline",
    default_args=default_args,
    schedule_interval="0 */2 * * *",  # toutes les 2 heures
    catchup=False,
    tags=["nifi", "spark"]
) as dag:

    nifi_task = PythonOperator(
        task_id="trigger_nifi",
        python_callable=start_nifi_flow
    )

    spark_task = PythonOperator(
        task_id="run_spark",
        python_callable=run_spark_job
    )

    # Ordonnancement : NiFi avant Spark
    nifi_task >> spark_task