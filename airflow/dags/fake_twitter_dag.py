import sys
sys.path.append("airflow")

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from work_definitions.operator.twitter_fake_operator import FakeTwitterOperator
from os.path import join
from pathlib import Path


with DAG(dag_id="FakeTwitterDAG", start_date=days_ago(2), schedule_interval="@daily") as dag:

    file_path = join("datalake/fake_twitter_datascience",
                     "extract_date={{ ds }}",
                     "datascience_{{ ds_nodash }}.json")


    def create_parent_folder(file_path):
        (Path(file_path).parent).mkdir(parents=True, exist_ok=True)


    task_create_folder_in_datalake = PythonOperator(
        task_id = "create_folder_in_datalake",
        python_callable = create_parent_folder,
        op_kwargs = {"file_path": file_path}
    )
    
    task_extract_data = FakeTwitterOperator(file_path=file_path, 
                                       query="datascience", 
                                       start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                       end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                       task_id="fake_twitter_datascience")
    
    
    task_create_folder_in_datalake >> task_extract_data