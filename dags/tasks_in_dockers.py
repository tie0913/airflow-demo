from datetime import datetime
from airflow.sdk import DAG, Param
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount

with DAG(
    dag_id="tasks_in_dockers",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo" , "training"],
    params={
        "vm_name": Param(
            type="string",
            pattern=r"^poc-[a-z0-9-]+$",
            title="VM Name",
            description="The name of the VM to be created. Must start with 'poc-' and can only contain lowercase letters, numbers, and hyphens.",
        ),
        "machine_type": Param(
            type="string",
            enum=[
                "e2-small",
                "e2-medium",
                "e2-standard-2",
                "e2-standard-4",
            ],
            title="Machine Type",
        ),
        "boot_disk_size_gb": Param(
            type="integer",
            minimum=10,
            maximum=200,
            title="Boot Disk Size (GB)",
        ),
    },
) as dag:
    load_data = DockerOperator(
        task_id="load_data",
        image="python:3.11-slim",
        command='python -c "print(\'1. Prepareing Data\')"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    create_vm_plan = DockerOperator(
        task_id="create_vm_plan",
        image="gcp-vm-tool:dev",
        command=[
            "--config",
            "/app/config/base.json",
            "--name",
            "{{ params.vm_name }}",
            "--machine-type",
            "{{ params.machine_type }}",
            "--boot-disk-size-gb",
            "{{ params.boot_disk_size_gb }}",
            "create",
            "--execute",
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/run/secrets/gcp-adc.json",
        },
        mounts=[
            Mount(
                source="/home/tiewang/.config/gcloud/application_default_credentials.json",
                target="/run/secrets/gcp-adc.json",
                type="bind",
                read_only=True,
            ),
        ],
        user="1000:1000",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        force_pull=False,
    )

    run_training = DockerOperator(
        task_id="run_training",
        image="python:3.11-slim",
        command='python -c "print(\'3. Training \')"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    cleanup_resources = DockerOperator(
        task_id="cleanup_resources",
        image="gcp-vm-tool:dev",
        command=[
            "--config",
            "/app/config/base.json",
            "--name",
            "{{ params.vm_name }}",
            "delete",
            "--execute",
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/run/secrets/gcp-adc.json",
        },
        mounts=[
            Mount(
                source="/home/tiewang/.config/gcloud/application_default_credentials.json",
                target="/run/secrets/gcp-adc.json",
                type="bind",
                read_only=True,
            ),
        ],
        user="1000:1000",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        force_pull=False,
        trigger_rule="all_done",
    )


    load_data >> create_vm_plan >> run_training >> cleanup_resources