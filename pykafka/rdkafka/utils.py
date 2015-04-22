import inspect


def get_defaults_dict(func):
    """
    Maps func argument names to default values

    Arguments lacking defaults will be missing entirely, as well as varargs
    and keyword args.
    """
    argspec = inspect.getargspec(func)
    indices = xrange(-len(argspec.defaults), 0)
    return {argspec.args[i]: argspec.defaults[i] for i in indices}
