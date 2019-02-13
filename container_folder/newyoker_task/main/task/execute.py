BASE_FOLDER = '/container_folder/newyoker_task'
import os, sys
sys.path.append(BASE_FOLDER)

from etl.utils.commons import read_file, get_base
from etl.task.main import Task


TASK = '/configs/decompress_and_clean.json'
#TASK = '/configs/sample_users_review.json'
#TASK = '/configs/sample_users_no_review_in_last_year.json'

class ExecuteTask():

    def _run_task(self, definition):
        task = Task(definition=definition,
                    name=definition['name'])
        task.run()

    def run(self):
        """
        Given the task file, runs all task using
        the ETL Task Object
        """
        self._run_task(read_file(get_base(os.path.realpath(__file__), 1) + TASK))

if __name__ == "__main__":
    execute = ExecuteTask()
    execute.run()