import logging


def get_logger_for_function(function):
    return logging.getLogger('.'.join((function.__module__, function.__name__)))
