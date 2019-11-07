"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 5),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("tutorial", default_args=default_args, schedule_interval=timedelta(1))


download_file = BashOperator(
    task_id="download_file",
    bash_command="curl https://ia600107.us.archive.org/27/items/stackexchange/Sites.xml -o /tmp/file.xml",
    dag=dag
)

alter_file = BashOperator(
    task_id="alter_file",
    bash_command="sed -e 's/row/pipoca/g' /tmp/file.xml > /tmp/saida.xml",
    dag=dag
)

split_file = BashOperator(
    task_id="split_file",
    bash_command="cd /tmp/; split -n 10 /tmp/saida.xml",
    dag=dag
)

download_file >> alter_file >> split_file


end_task = DummyOperator(
    task_id="end_task",
    dag=dag
)

files = ["xaj", "xai", "xah", "xag", "xaf", "xae", "xad", "xac", "xab", "xaa"]


for i in files:

    remove_dir = BashOperator(
        task_id="remove_dir_%s" % (i),
        bash_command="rm -fr /tmp/dir_%s" % (i),
        dag=dag
    )

    mkdir_dir = BashOperator(
        task_id="mkdir_dir_%s" % (i),
        bash_command="mkdir /tmp/dir_%s" % (i),
        dag=dag
    )

    move_file = BashOperator(
        task_id="move_file_%s" % (i),
        bash_command="mv /tmp/%s /tmp/dir_%s" % (i, i),
        dag=dag
    )

    split_file >> remove_dir >> mkdir_dir >> move_file >> end_task

