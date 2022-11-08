from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def _process(portainer_key, remove_url):
    r = requests.delete(
        url=remove_url,
        headers={
            'x-api-key' : portainer_key,
            'Content-Type':'application/json'
        },
        data='{}',
        verify=False
    )
    r.raise_for_status()

def build_remove_image_task(dag: DAG, repo_name) -> PythonOperator:

    task_remove_image = PythonOperator(
        task_id='image_remove',
        python_callable=_process,
        op_kwargs={
            'portainer_key':"{{ var.value.portainer_api_key }}",
            'remove_url': "{{ var.value.docker_base_remove_url }}/" + repo_name + "-requirements:latest"
        },
        dag=dag,
        trigger_rule='one_success',
    )

    return task_remove_image