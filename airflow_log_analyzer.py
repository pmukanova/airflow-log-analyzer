from airflow import DAG
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date' : datetime(2021,10,13)
}

log_dag = DAG(
    dag_id = "log_analyzer",
    default_args = default_args,
    description = "Analyze logs from marketvol DAG",
    schedule_interval = "0 19 * * 1-5"
)

log_dir = "/opt/airflow/logs/marketvol"
file_list = Path(log_dir).rglob('*.log')

def analyze_file(**context):
    
    symbol = context["symbol"]
    log_file_list = list(file_list)
    error_list = []
    error_count = 0 

    for file in log_file_list:
        if str(file).find(symbol) != -1:
            with open(file, "r") as reading_log_file:
                for line in reading_log_file:
                    if "ERROR" in line:
                        error_count = error_count + 1
                        error_list.append(line)
    
    task_instance = context["task_instance"]
    task_instance.xcom_push(key = "error_count", value = error_count)
    task_instance.xcom_push(key = "error_list", value = error_list)

def print_error_log(**context):
    error_count = context["ti"].xcom_pull(key="error_count")
    print("Total Error count for ", context["symbol"], error_count)
    error_list = context["ti"].xcom_pull(task_ids="tsla_log_errors", key="error_list")
    print("Error list:", error_list)
    
    return (error_count, error_list)

task1 = PythonOperator(
    task_id = "aapl_log_errors",
    python_callable = analyze_file,
    provide_context = True,
    op_kwargs = {"symbol": "AAPL"},
    do_xcom_push = True,
    dag = log_dag
)

task2 = PythonOperator(
    task_id = "tsla_log_errors",
    python_callable = analyze_file,
    provide_context = True,
    op_kwargs = {"symbol": "TSLA"},
    do_xcom_push = True,
    dag = log_dag
)
task3 = PythonOperator(
    task_id = "print_aapl_errors",
    python_callable = print_error_log,
    provide_context = True,
    op_kwargs = {"symbol" : "AAPL"},
    dag = log_dag
)

task4 = PythonOperator(
    task_id = "print_tsla_errors",
    python_callable  = print_error_log,
    provide_context = True,
    op_kwargs = {"symbol" : "TSLA"},
    dag = log_dag
)

task2 >> task4
task1 >> task3
 
