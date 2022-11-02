from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import paramiko
import io

class UpdateImageError(Exception): pass

def update_docker_image_from_host_via_ssh(host_server_key, host_server_url, repo_name, moll_url, docker_registry):
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
    dockerfile = f'/opt/airflow/repos/{repo_name}/'
    docker_registry_tag = f'{docker_registry}/{repo_name}:latest'
    docker_build_push_script = '/opt/airflow/repos/docker-build-push/docker-build-push.py'

    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    keyfile = io.StringIO(host_server_key)
    mykey = paramiko.RSAKey.from_private_key(keyfile)
    p.connect(host_server_url, port=2200, username="airflow", pkey=mykey)
    cmd = f"python3 {docker_build_push_script} {moll_url} {dockerfile} {docker_registry_tag}"
    print(f"Command: {cmd}")
    stdin, stdout, stderr = p.exec_command(cmd)
    # assuming output is sent to stdout and exceptions to stderr
    # TODO parse result for raises
    txt_stderr = stderr.readlines()
    print(txt_stderr)
    if txt_stderr:
        raise UpdateImageError(txt_stderr)
    return 0


def build_update_image_task(dag: DAG, repo_name) -> PythonOperator:
    update_image_task = PythonOperator(
        task_id='update_docker_image',
        python_callable=update_docker_image_from_host_via_ssh,
        op_kwargs={
            "repo_name": repo_name,
            "host_server_url" : "{{ var.value.repo_server_url }}",
            "host_server_key": "{{ var.value.repo_server_key }}",
            "moll_url" : "{{ var.value.generic_moll_url }}",
            "docker_registry": "{{ conn.somenergia_registry.host }}"
        },
        trigger_rule='one_success',
        dag=dag,
    )

    return update_image_task
