BASE_FOLDER = '/container_folder/newyorker_task'
TASK_FOLDER = '/task'
DAG_FOLDER = '/configs/'
import os, sys
sys.path.append(BASE_FOLDER)

"""Task Modules"""
from etl.utils.commons import read_file, get_base
from main.dag import DAG_MAPPER
from etl.dag.main import Dag

DEFAULT_TAR_FILE_NAME = 'yelp_dataset.tar'

class ExecuteDag():

    def __init__(self, dag=None):
        assert dag is not None and dag in DAG_MAPPER, "DAG configuration argument is not found!"
        self.dag_path = DAG_FOLDER + DAG_MAPPER[dag]

    def _start_dag(self, definition):
        if 'decompress_and_clean' in definition:
            definition['decompress_and_clean']['filename'] = DEFAULT_TAR_FILE_NAME
            if len(sys.argv) > 2:
                definition['decompress_and_clean']['filename'] = sys.argv[2]
        dag = Dag(definition, get_base(os.path.realpath(__file__), 2)
                                            + TASK_FOLDER)
        dag.run()

    def run(self):
        """
        Given the dag file,
        starts the pipeline
        """
        self._start_dag(read_file(get_base(os.path.realpath(__file__), 1) + self.dag_path))


if __name__ == "__main__":
    dag = None
    if len(sys.argv) > 0:
        dag = sys.argv[1]
    execute = ExecuteDag(dag)
    execute.run()