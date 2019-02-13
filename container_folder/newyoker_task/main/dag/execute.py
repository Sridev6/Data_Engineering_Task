BASE_FOLDER = '/container_folder/newyoker_task'
TASK_FOLDER = '/task'
import os, sys
sys.path.append(BASE_FOLDER)

from etl.utils.commons import read_file, get_base
from etl.dag.main import Dag

DAG = '/configs/simple_pipe.json'

class ExecuteDag():

    def _start_dag(self, definition):
        dag = Dag(definition, get_base(os.path.realpath(__file__), 2)
                                            + TASK_FOLDER)
        dag.run()

    def run(self):
        """
        Given the dag file,
        starts the pipeline
        """
        self._start_dag(read_file(get_base(os.path.realpath(__file__), 1) + DAG))


if __name__ == "__main__":
    execute = ExecuteDag()
    execute.run()