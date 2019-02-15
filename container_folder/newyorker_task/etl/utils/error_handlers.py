import sys, traceback
from functools import wraps

from etl import DEFAULT_EMAIL_RECEPIENTS
from etl.utils.notify_email import send_mail

def handle_warnings(func):
    """ Notifies a Warning during the flow.
        Does not stop the script.
    """
    @wraps(func)
    def wrap_over_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as warning:
            print(warning)

    return wrap_over_func

def handle_errors(func):
    """ Notifies an Error during the flow.
        Stops the script.
    """
    @wraps(func)
    def wrap_over_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as error:
            raise error
            #send_mail(DEFAULT_EMAIL_RECEPIENTS, error)
            #raise error
    return wrap_over_func

def handle_task_errors(func):
    """ Notifies an Error during the task flow.
            Stops the script.
    """

    @wraps(func)
    def wrap_over_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as error:
            _, exc_value, exc_traceback = sys.exc_info()
            send_mail(DEFAULT_EMAIL_RECEPIENTS,
                      '\n'.join(traceback.format_list(traceback.extract_tb(exc_traceback))) + str(exc_value), args[1])
            raise error

    return wrap_over_func