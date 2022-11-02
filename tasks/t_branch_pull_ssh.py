from airflow.operators.python_operator import BranchPythonOperator
from airflow import DAG
import paramiko
import io


def pull_repo_ssh(repo_github_name, repo_server_url, repo_server_key, task_name):
    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    keyfile = io.StringIO(repo_server_key)
    mykey = paramiko.RSAKey.from_private_key(keyfile)
    p.connect(repo_server_url, port=2200, username="airflow", pkey=mykey)
    _, _, stderr = "git -C /opt/airflow/repos/{repo_github_name} diff origin/main -- requirements.txt"
    txt_stderr = stderr.readlines()
    txt_stderr = "".join(txt_stderr)
    requirements_updated = len(txt_stderr) > 0
    if requirements_updated:
        print (f"Stderr de git diff requirements retornat {txt_stderr} and requires image update")
    else:
        print (f"Stderr de git diff requirements no ha retornat cap missatge {txt_stderr}")
    stdin, stdout, stderr = p.exec_command(f"git -C /opt/airflow/repos/{repo_github_name} pull")
    txt_stderr = stderr.readlines()
    txt_stderr = "".join(txt_stderr)
    print (f"Stderr de git pull ha retornat {txt_stderr}")
    # si stderr té més de 0 \n és que hi ha canvis al fer pull
    #Your configuration specifies to merge with the ref 'refs/heads/main' from the remote, but no such ref was fetched.
    #Apareix quan fem molts git pull a la vegada

    # image removal and build is not working atm
    if '{{ dag.dag_id }}' == 'hs_conversations_dag':
        return 'update_docker_image' if requirements_updated else task_name
    else:
        return task_name

def build_branch_pull_ssh_task(dag: DAG, task_name, repo_github_name) -> BranchPythonOperator:
    branch_pull_ssh_task = BranchPythonOperator(
        task_id='git_pull_task',
        python_callable=pull_repo_ssh,
        op_kwargs={ "repo_github_name": repo_github_name,
                    "repo_server_url": "{{ var.value.repo_server_url }}",
                    "repo_server_key": "{{ var.value.repo_server_key }}",
                    "task_name": task_name},
        do_xcom_push=False,
        dag=dag
    )

    return branch_pull_ssh_task
