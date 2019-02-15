import uuid
from datetime import datetime
from importlib import import_module

"""Task Modules"""
from etl.utils.error_handlers import handle_errors, handle_task_errors
from etl.task import STATUSES, MODULE_FOLDER, MODULE_CLASS

class Task():

    def __init__(self, definition=None, name=None, dependencies=None):
        self.id = name + '-' + str(uuid.uuid4())
        self.name = name
        self.task_definition = definition
        self.dependencies = dependencies
        self.start = datetime.now()
        self.end = None
        self.statuses =STATUSES
        self.current_status = self.statuses['idle']
        self.modules_folder = MODULE_FOLDER
        self.module_class = MODULE_CLASS
        self.task_module = self.get_task_module()

    def _get_module_file(self, module_name):
        ''' Import file module

        :param module_name: Module file name

        :return: file object of module
        '''
        return import_module(module_name)

    def _get_module(self, module_file, module_name):
        ''' Import module object

        :param module_file: file name of module
        :param module_name: class name of module

        :return: class object of module
        '''
        return getattr(module_file, module_name)

    @handle_errors
    def get_task_module(self):
        assert 'name' in self.task_definition, "Task Module not specified!"
        try:
            return self._get_module(self._get_module_file(self.modules_folder + self.task_definition['name']),
                                    self.module_class)
        except Exception as e:
            self.current_status = self.statuses['fail']
            raise e

    @handle_task_errors
    def run(self, task_name):
        """
        Executes any task given the
        underlying function named as "run"
        """
        self.current_status = self.statuses['run']
        task = self.task_module(self.task_definition)
        task.run()
        self.current_status = self.statuses['done']
        self.end = datetime.now()