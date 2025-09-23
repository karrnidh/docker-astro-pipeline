'''
Task flow api allows us to use decorators instead of operators such as PythonOperator
Task 1: to start with a number (eg 100)
Task 2: add 50 to the number
Task 3: Multiply the result by 2
Task 4: To divide the result by 10
'''

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="arithmetic_tfapi"
) as dag:
    @task
    def start_num():
        initial_value=100
        print(f"starting number: {initial_value}")
        return initial_value
    
    @task
    def addition(num):
        new_value= num+50
        print(f"Add fifty: {num} + 50 = {new_value}")
        return new_value
    
    @task
    def multiply(num):
        new_value= num * 2
        print(f"MMultiply by 2: {num} * 2 = {new_value} ")
        return new_value
    
    @task
    def divide(num):
        new_value= num /10
        print(f"Divide by 10: {num} / 10 = {new_value}")
        return new_value
    
    #dependencies
    start_val= start_num()
    additions= addition(start_val)
    multiplication= multiply(additions)
    dividion= divide(multiplication)

