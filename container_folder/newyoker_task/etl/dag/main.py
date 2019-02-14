import json
from concurrent.futures import ThreadPoolExecutor, as_completed

"""Task Modules"""
from etl.task.main import Task
from etl.utils.commons import read_file, diff_lists
from etl.dag import TASK_PARALLEL
from etl.dag.schedule import schedule_task


class Dag():

    def __init__(self, dag, task_path):
        self.pipeline_definition = dag
        self.tasks = self._get_tasks(task_path)
        self._scheduled_tasks = []
        self.waiting_tasks = self.tasks
        self.tasks_to_schedule = self.get_tasks_to_schedule()
        self.running_tasks = []
        self.completed_tasks = []

    def _get_tasks(self, base_path):
        """ Get the set of initial non dependency  tasks """
        tasks = []
        for job, info in self.pipeline_definition.items():
            if 'dependencies' not in info:
                self.pipeline_definition[job]['dependencies'] = None
            task_definition = read_file(info['path'].format(task=base_path))
            if job == 'decompress_and_clean':
                task_definition['filename'] = task_definition['filename'].format(
                    tar_filename=self.pipeline_definition[job]['filename'])
            tasks.append(Task(definition=task_definition,
                              name=job,
                              dependencies=info['dependencies']))
        return tasks

    def get_tasks_to_schedule(self, completed_task=None):
        """ Get tasks that has the latest completed task as it's dependency task  """
        tasks_to_schedule = []

        # tasks which do not wait for any task first
        no_wait_tasks = [task for task in self.waiting_tasks if not task.dependencies]
        for task in no_wait_tasks:
            tasks_to_schedule.append(task)

        if completed_task:
            for task in self.waiting_tasks:
                # get all completed task filenames
                completed_filenames = [t.name for t in self.completed_tasks]
                # check all tasks in _wait_for_tasks are completed
                if all(t in completed_filenames for t in task.dependencies):
                    tasks_to_schedule.append(task)
                else:
                    task.current_status = task.statuses['denied']

        self._set_scheduled_tasks(tasks_to_schedule)

        return tuple(tasks_to_schedule)

    def _set_scheduled_tasks(self, tasks_to_schedule):
        """ Cumulative of tasks to schedule """
        for task in tasks_to_schedule:
            self._scheduled_tasks.append(task)
        self._set_waiting_tasks(self._scheduled_tasks)

    def _set_waiting_tasks(self, scheduled_tasks):
        """ Cumulative of tasks waiting to be scheduled """
        self.waiting_tasks = diff_lists(self.tasks, scheduled_tasks)

    def run(self):
        with ThreadPoolExecutor(max_workers=TASK_PARALLEL) as executor:
            futures = {
                executor.submit(schedule_task, task)
                : task for task in self.tasks_to_schedule
            }

            # tasks_to_schedule is a tuple (immutable data-type)
            # passing it into a list makes a deep copy instead of a pure reference
            # and allows for appending & removing items
            self.running_tasks = list(self.tasks_to_schedule)
            while self.running_tasks:
                # get first completed future
                f = next(as_completed(futures))
                # completed_task is a <Task object>
                completed_task = f.result()
                del futures[f]
                # add completed_task to list of completed tasks
                self.completed_tasks.append(completed_task)
                # remove completed_task from running_tasks
                self.running_tasks = [t for t in self.running_tasks if t.id != completed_task.id]
                # check for available-to-schedule tasks
                tasks_to_schedule = self.get_tasks_to_schedule(completed_task=completed_task)
                # each task to run is a <Task object>
                for task in tasks_to_schedule:
                    self.running_tasks.append(task)
                    # submit a new future for the now available-to-schedule task
                    futures[executor.submit(schedule_task, task)] = task




