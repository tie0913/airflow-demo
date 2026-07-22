from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="demo_training_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "training"],
):

    @task
    def load_data():
        print("1. 加载数据")

    @task
    def prepare_model():
        print("2. 准备训练模型")

    @task
    def run_training():
        print("3. 执行训练")

    @task
    def cleanup_resources():
        print("4. 回收资源")

    load = load_data()
    prepare = prepare_model()
    train = run_training()
    cleanup = cleanup_resources()

    load >> prepare >> train >> cleanup
