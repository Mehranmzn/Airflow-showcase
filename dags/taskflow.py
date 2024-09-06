from airflow.decorators import dag, task
from datetime import datetime





@dag(
    schedule_interval='@daily', 
    start_date=datetime(2023, 1, 1),
    catchup = False,
    tags = ['taskflow']
    )


def taskflow():

    @task()
    def task_a():
        print("Task A")
        return 42

    @task()
    def task_b(value):
        print(f"Task B")
        print(f"Value: {value}")
        return(value)


    task_b(task_a())


taskflow()    
