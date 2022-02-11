import traceback


def get_error_message(exception):
    return repr(exception)


def get_error_and_traceback_message(exception):
    return repr(exception) + "\n\n" + traceback.format_exc()


# ===================================================================================
# Connections
# ===================================================================================

class ConnectionNotOpenException(Exception):
    pass


class ConnectionOpenException(Exception):
    pass


class ConnectionReadTimeoutException(Exception):
    pass


class ConnectionWriteException(Exception):
    pass


# ===================================================================================
# MQTT
# ===================================================================================

class MqttConnectionTimeoutException(Exception):
    pass


class MqttConnectionSingleSubscribeTimeoutException(Exception):
    pass


# ===================================================================================
# Files
# ===================================================================================

class FileDoesNotExist(Exception):
    pass
