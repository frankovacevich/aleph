import traceback


class Exceptions:

    # Connections
    class InvalidKey(Exception): pass
    class ConnectionReadingTimeout(Exception): pass
    class ConnectionOpeningTimeout(Exception): pass
    class ConnectionWritingTimeout(Exception): pass
    class ConnectionNotOpen(Exception): pass

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
        if args_str !=  "": return repr(self.exception) + "\n" + args_str
        return repr(self.exception)

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
