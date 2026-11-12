# Test certificates

For tests only: the private keys are public. The samples runner mounts these into the Temporal
server container it starts for samples that set `.docker_server` in their `sample.zon`, and passes
their paths to the samples.

- `ca.pem`: the CA that signed the other two. Its key was discarded.
- `server.pem`, `server.key`: the server's certificate, for `localhost` and `127.0.0.1`.
- `client.pem`, `client.key`: a client certificate the server accepts.

They are valid until 2126. `generate.sh` replaces all of them.
