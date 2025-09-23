'''
Task 1: to start with a number (eg 100)
Task 2: add 50 to the number
Task 3: Multiply the result by 2
Task 4: To divide the result by 10
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_number(**context):
    context["ti"].xcom_push(key="current_value",value=100) #cross communication= xcom ; can push or pull
    print("Starting number is 100")

def add_num(**context):
    current_value=context["ti"].xcom_pull(key="current_value", task_ids="start_number")
    new_value= current_value+50
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"add 50: {current_value} + 50 = {new_value}")

def multiply(**context):
    current_value=context["ti"].xcom_pull(key="current_value",task_ids="add_num")
    new_value= current_value*2
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"multiply by 2: {current_value} * 2 = {new_value}")

def divide(**context):
    current_value= context["ti"].xcom_pull(key="current_value", task_ids="multiply")
    new_value= current_value/10
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"divide by 10: {current_value} / 10 = {new_value}")
#define the DAG

with DAG(
    dag_id= "arithmetic_operations"
) as dag:
    start_number= PythonOperator(
        task_id= "start_number",
        python_callable= start_number,
        #provide_context= True
    )

    add_num= PythonOperator(
        task_id= "add_num",
        python_callable= add_num,
        # provide_context= True
    )

    multiply= PythonOperator(
        task_id= "multiply",
        python_callable= multiply,
        # provide_context= True
    )

    divide= PythonOperator(
        task_id= "divide",
        python_callable= divide,
        # provide_context= True
    )

#dependencies
start_number >> add_num >> multiply >> divide