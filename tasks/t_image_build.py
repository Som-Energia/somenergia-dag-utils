from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def _process(repo_name, portainer_key, post_url):
    r = requests.post(
            url=post_url,
            headers={
                'x-api-key' : portainer_key,
                'Content-Type':'application/json'
            },
            params={
                't':'somenergia-jardiner-carter-requirements:latest',
                'remote': f'https://github.com/Som-Energia/{repo_name}.git#main',
                'nocache': 'true'
            },
            data='{}',
            verify=False
    )
    r.raise_for_status()

def build_image_build_task(dag: DAG, repo_name) -> PythonOperator:

    task_image_build = PythonOperator(
        task_id='image_build',
        python_callable=_process,
        op_kwargs={
            'repo_name': repo_name,
            'portainer_key': '{{ var.value.portainer_api_key }}',
            'post_url': '{{ var.value.docker_build_url }}'
        },
        trigger_rule='one_success',
        dag=dag
    )

    return task_image_build
