from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import paramiko
import io

def clone_repo_via_ssh(repo_github_name, repo_server_url, repo_server_key):
    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    keyfile = io.StringIO(repo_server_key)
    mykey = paramiko.RSAKey.from_private_key(keyfile)
    p.connect(repo_server_url, port=2200, username="airflow", pkey=mykey)
    p.exec_command(f"git clone https://github.com/Som-Energia/{repo_github_name}.git /opt/airflow/repos/{repo_github_name}")

def build_git_clone_ssh_task(dag: DAG, repo_github_name) -> PythonOperator:
    git_clone_ssh_task = PythonOperator(
        task_id='git_clone_ssh_task',
        python_callable=clone_repo_via_ssh,
        op_kwargs={ "repo_github_name": repo_github_name,
                    "repo_server_url" : "{{ var.value.repo_server_url }}",
                    "repo_server_key": "{{ var.value.repo_server_key }}" },
        dag=dag,
    )

    return git_clone_ssh_task
