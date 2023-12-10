from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(schedule_interval=None, start_date=datetime(2023, 12, 1), catchup=False)
def fakestore_pipeline_elt():
    @task
    def get_data():
        return [1, 2, 3]

    @task
    def transform_data(data):
        return [x * 2 for x in data]

    @task
    def store_data(data):
        print(data)

    data = get_data()
    transformed_data = transform_data(data)
    store_data(transformed_data)

fakestore_pipeline_elt()