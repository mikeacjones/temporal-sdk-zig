`client.Client.connect` connects to a server and namespace with `client.Options`: address, TLS and mutual TLS, API key, headers, gRPC retry policy, `rpc_timeout`, codecs, interceptors and metrics. `Client.checkHealth` checks the connection.
`client.loadOptions` and `client.loadDefaultOptions` read connection settings from a Temporal config file profile and `TEMPORAL_*` environment variables.
`Client.updateApiKey`, `Client.updateHeaders` and `Client.reconnect` change a connected client's credentials, headers or connection.
