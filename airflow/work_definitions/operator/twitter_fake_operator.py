import sys
sys.path.append("airflow")

from work_definitions.hook.fake_twitter_hook import FakeTwitterHook
from airflow.models import BaseOperator

import json


class FakeTwitterOperator(BaseOperator):

    template_fields = ["query", "file_path", "start_time", "end_time"]
    
    def __init__(self, file_path, query, start_time, end_time, **kwargs):
        self.file_path = file_path
        self.query = query
        self.start_time = start_time
        self.end_time = end_time

        super().__init__(**kwargs)

    def execute(self, context):
        with open(self.file_path, "w") as outuput_file:
            for pg in FakeTwitterHook(self.query, self.start_time, self.end_time).run():
                json.dump(pg, outuput_file, ensure_ascii=False)
                outuput_file.write("\n")
