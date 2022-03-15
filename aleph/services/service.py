
class Service:

    # Keys
    read_request_keys = []
    namespace_subs_keys = []
    connection_subs_keys = []

    # Connections
    namespace_connection = None
    connection = None

    def __init__(self, service_id):
        self.service_id = service_id

    # ===================================================================================
    # Main events (override me)
    # ===================================================================================
    def setup(self):
        return

    def on_read_request(self, key, **kwargs):
        return self.connection.safe_read(key, **kwargs)

    def on_new_data_from_namespace(self, key, data):
        self.connection.safe_write(key, data)

    def on_new_data_from_connection(self, key, data):
        self.namespace_connection.safe_write(key, data)

    # ===================================================================================
    # Error handling (override me)
    # ===================================================================================
    def on_status_change(self, status_code):
        """
        0: both connection and namespace_connection are connected
        1: connection is not connected
        2: namespace_connection is not connected
        3: neither is connected
        """
        return

    def on_connection_read_error(self, error):
        return

    def on_connection_write_error(self, error):
        return

    def on_namespace_read_error(self, error):
        return

    def on_namespace_write_error(self, error):
        return

    def on_read_request_error(self, error):
        return

    # ===================================================================================
    # Start read requests server
    # ===================================================================================
    def accept_read_requests(self):
        return

    # ===================================================================================
    # Private
    # ===================================================================================
    def __on_status_change__(self):
        status_0 = self.namespace_connection.is_connected()
        status_1 = self.connection.is_connected()
        if status_0 and status_1: self.on_status_change(0)
        elif status_0 and not status_1: self.on_status_change(0)
        elif not status_0 and status_1: self.on_status_change(0)
        else: self.on_status_change(3)
        return

    # ===================================================================================
    # Use me
    # ===================================================================================
    def run(self):

        # Main callbacks
        self.namespace_connection.on_new_data = self.on_new_data_from_namespace
        self.connection.on_new_data = self.on_new_data_from_connection

        # Error callbacks
        self.namespace_connection.on_read_error = self.on_namespace_read_error
        self.namespace_connection.on_write_error = self.on_namespace_write_error
        self.connection.on_read_error = self.on_connection_read_error
        self.connection.on_write_error = self.on_connection_write_error

        # Status change callbacks
        self.namespace_connection.on_connect = self.__on_status_change__
        self.namespace_connection.on_disconnect = self.__on_status_change__
        self.connection.on_connect = self.__on_status_change__
        self.connection.on_disconnect = self.__on_status_change__

        # Open connections
        self.namespace_connection.open_async()
        self.connection.open_async()

        # Subscribe to keys
        for key in self.namespace_subs_keys: self.namespace_connection.subscribe_async(key)
        for key in self.connection_subs_keys: self.connection.subscribe_async(key)

        # Custom setup
        self.setup()

        # Load accept read requests server
        self.accept_read_requests()

        # Run
        while True:
            pass
