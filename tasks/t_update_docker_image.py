from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import paramiko
import io

def update_docker_image_from_host_via_ssh(repo_server_key, repo_server_url, repo_name, moll_url, docker_registry):
    '''
    airflow is running in its container, so we need to connect to the host
    which has the python docker script that works.
    the python script:
    - logins to a docker daemon of a moll
    - tells the moll to build the image given the DockerFile of the repo which is cloned in the host
    - tells the moll to push the image to our private registry

    Then, the task will run in a DockerOperator who will download the image from the private registry
    if it doesn't have it. TODO: how will other molls know they have to update it?
    '''
    dockerfile = f'/opt/airflow/repos/{repo_name}/Dockerfile'
    docker_registry_tag = f'{docker_registry}/{repo_name}'
    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    keyfile = io.StringIO(repo_server_key)
    mykey = paramiko.RSAKey.from_private_key(keyfile)
    p.connect(repo_server_url, port=2200, username="airflow", pkey=mykey)
    result = p.exec_command(f"python3 /opt/airflow/repos/docker-build-push/docker-build-push.py {moll_url} {dockerfile} {docker_registry_tag}")
    print(result)
    # TODO parse result for raises
    return 0


def build_update_image_task(dag: DAG, repo_name) -> PythonOperator:
    update_image_task = PythonOperator(
        task_id='git_clone_ssh_task',
        python_callable=update_docker_image_from_host_via_ssh,
        op_kwargs={
            "repo_name": repo_name,
            "host_server_url" : "{{ var.value.repo_server_url }}",
            "host_server_key": "{{ var.value.repo_server_key }}",
            "moll_url" : "{{ var.value.generic_moll_url }}",
            "docker_registry": "{{ var.value.somenergia_docker_registry }}"
        },
        dag=dag,
    )

    return update_image_task
