from .service import Service


class GatewayService(Service):

    local_root_key = ""
    remote_root_key = ""
    local_namespace_connection = None
    remote_namespace_connection = None

    def __init__(self, service_id):
        super().__init__(service_id)

        # Load features
        self.namespace_connection = self.local_namespace_connection
        self.connection = self.remote_namespace_connection
        self.local_namespace_connection.persistent = True
        self.remote_namespace_connection.persistent = True

        # Load keys
        if not self.local_root_key.endswith("#"): self.local_root_key += ".#"
        if not self.remote_root_key.endswith("#"): self.remote_root_key += ".#"
        self.namespace_subs_keys = [self.local_root_key]
        self.connection_subs_keys = [self.remote_root_key]

    # On read request
    def on_read_request(self, key, **kwargs):
        if key.startswith(self.local_root_key[:-2]):
            return self.local_namespace_connection.safe_read(key, **kwargs)
        else:
            return self.remote_namespace_connection.safe_read(key, **kwargs)
