from airflow import DAG
from airflow.operators.python import PythonOperator

from src.preprocessing.preprocessing import run_preprocessing

dag = DAG(
    max_active_runs=1,
    dag_id="enrichment_invoices",
)

preprocessing = PythonOperator(task_id="run_preprocessing", python_callable=run_preprocessing, dag=dag)
preprocessing
