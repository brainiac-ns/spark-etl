import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def fun1():
    print("FUN1")


default_args = {
    "owner": "SCM CORE Team",
    "start_date": datetime.datetime(2023, 5, 26, 13, 15),
    "depends_on_past": False,
    "retries": 0,
}

dag = DAG(
    max_active_runs=1, dag_id="enrichment_invoices", default_args=default_args, schedule_interval="*/10 * * * * *"
)

preprocessing = PythonOperator(task_id="run_preprocessing", python_callable=fun1, dag=dag)
ifa = PythonOperator(task_id="run_ifa", python_callable=fun1, dag=dag)
buo = PythonOperator(task_id="run_buo", python_callable=fun1, dag=dag)
esn = PythonOperator(task_id="run_esn", python_callable=fun1, dag=dag)

preprocessing >> [ifa, buo] >> esn
