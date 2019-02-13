import logging
from functools import wraps


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

    return wrap_over_func