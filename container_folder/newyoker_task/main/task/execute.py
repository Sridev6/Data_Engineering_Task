BASE_FOLDER = '/container_folder/newyoker_task'
TASK_FOLDER = '/configs/'
import os, sys
sys.path.append(BASE_FOLDER)

"""Task Modules"""
from etl.utils.commons import read_file, get_base
from main.task import TASK_MAPPER
from etl.task.main import Task


class ExecuteTask():

    def __init__(self, task=None):
        assert task is not None and task in TASK_MAPPER, "Task configuration argument is not found!"
        self.task_path = TASK_FOLDER + TASK_MAPPER[task]

    def _run_task(self, definition):
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