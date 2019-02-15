def schedule_task(task):
    """ Run the task object and return the task as completed task """
    task.run(task.name)
    return task