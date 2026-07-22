from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="demo_training_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo" , "training"],
) as dag:

    load_data = DockerOperator(
        task_id="load_data",
        image="python:3.11-slim",
        command='python -c "print(\'1. 加载数据\')"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    prepare_model = DockerOperator(
        task_id="prepare_model",
        image="python:3.11-slim",
        command='python -c "print(\'2. 准备训练模型\')"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    run_training = DockerOperator(
        task_id="run_training",
        image="python:3.11-slim",
        command='python -c "print(\'3. 执行训练\')"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    cleanup_resources = DockerOperator(
        task_id="cleanup_resources",
        image="python:3.11-slim",
        command='python -c "print(\'4. 回收资源\')"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    load_data >> prepare_model >> run_training >> cleanup_resources