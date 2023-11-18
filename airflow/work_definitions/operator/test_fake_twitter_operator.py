import sys
sys.path.append("work_definitions")

from work_definitions.hook.fake_twitter_hook import FakeTwitterHook
from airflow.models import BaseOperator, DAG, TaskInstance
from os.path import join
from pathlib import Path

import pendulum
import json


class FakeTwitterOperatorTEST(BaseOperator):
    
    def __init__(self, file_path, query, start_time, end_time, **kwargs):
        self.file_path = file_path
        self.query = query
        self.start_time = start_time
        self.end_time = end_time

        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()

        with open(self.file_path, "w") as outuput_file:
            for pg in FakeTwitterHook(self.query, self.start_time, self.end_time).run():
                json.dump(pg, outuput_file, ensure_ascii=False)
                outuput_file.write("\n")


if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = pendulum.now().strftime(TIMESTAMP_FORMAT)
    start_time = pendulum.today().subtract(days = -3).strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    file_path = join("datalake/fake_twitter_datascience",
                     f"extract_date={pendulum.now().date()}",
                     f"datascience_{pendulum.now().strftime('%Y%m%d')}.json")

    with DAG(dag_id = "FakeTwitterTestOperator", start_date=pendulum.now()) as dag:
        op = FakeTwitterOperatorTEST(file_path=file_path, query=query, start_time=start_time, end_time=end_time,
                                 task_id="test_run_operator")
        
        ti = TaskInstance(task=op)
        
        op.execute(ti.task_id)