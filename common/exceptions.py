import traceback


class Exceptions:

    # Connections
    class ConnectionNotOpen(Exception): pass
    class InvalidKey(Exception): pass
    class ReadError(Exception): pass
    class ReadTimeout(Exception): pass
    class WriteError(Exception): pass

    # Services
    class ServiceInitError(Exception): pass


class Error:
    """
    Class for error handling.
    """

    def __init__(self, exception, **kwargs):
        self.exception = exception
        self.args = kwargs

    def message(self):
        args_str = "".join([str(x).title() + ": " + str(self.args[x]) + "\n" for x in self.args])
        return repr(self.exception) + "\n" + args_str

    def traceback(self):
        return traceback.format_exc()

    def message_and_traceback(self):
        return self.message() + "\n\n" + self.traceback()

    def raise_exception(self):
        raise self.exception

    def __str__(self):
        return self.message_and_traceback()

    @staticmethod
    def connection_error(e): return Error(e)
