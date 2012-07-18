class ImproperlyConfigured(Exception):
    pass

# Protocol Client Exceptions

class UnknownError(Exception):
    ERROR_CODE = -1


class OffsetOutOfRange(Exception):
    ERROR_CODE = 1


class InvalidMessage(Exception):
    ERROR_CODE = 2


class WrongPartition(Exception):
    ERROR_CODE = 3


class InvalidFetchSize(Exception):
    ERROR_CODE = 4


ERROR_CODES = dict((exc.ERROR_CODE, exc) for exc in (UnknownError,
    OffsetOutOfRange, InvalidMessage, WrongPartition, InvalidMessage))
