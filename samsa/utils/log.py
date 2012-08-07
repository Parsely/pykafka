import logging


def get_logger_for_function(function):
    name = '.'.join((function.__module__, function.__name__))
    return logging.getLogger(name)
