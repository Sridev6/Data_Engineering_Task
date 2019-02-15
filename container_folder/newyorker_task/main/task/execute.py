BASE_FOLDER = '/container_folder/newyorker_task'
TASK_FOLDER = '/configs/'
import os, sys
sys.path.append(BASE_FOLDER)

"""Task Modules"""
from etl.utils.commons import read_file, get_base
from main.task import TASK_MAPPER
from etl.task.main import Task

DEFAULT_TAR_FILE_NAME = 'yelp_dataset.tar'

class ExecuteTask():

    def __init__(self, task=None):
        assert task is not None and task in TASK_MAPPER, "Task configuration argument is not found!"
        self.tar_filename = DEFAULT_TAR_FILE_NAME
        self.task_path = TASK_FOLDER + TASK_MAPPER[task]
        if task in ['review', 'user'] and len(sys.argv) > 2:
            self.tar_filename = sys.argv[2]


    def _run_task(self, definition):
        if definition['name'] == 'decompress_and_clean':
            definition['filename'] = definition['filename'].format(tar_filename=self.tar_filename)
        task = Task(definition=definition,
                    name=definition['name'])
        task.run(task.name)

    def run(self):
        """
        Given the task file, runs all task using
        the ETL Task Object
        """
        self._run_task(read_file(get_base(os.path.realpath(__file__), 1) + self.task_path))

if __name__ == "__main__":
    task = None
    if len(sys.argv) > 0:
        task = sys.argv[1]
    execute = ExecuteTask(task)
    execute.run()