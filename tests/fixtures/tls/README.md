# Local TLS test fixtures

`ca.pem` is a test-only CA certificate. `localhost.p12` contains a server
certificate for DNS `localhost`, its deliberately public test private key,
and the CA certificate. Its password is `telekinesis-test`.

These fixtures are exclusively for loopback transport tests. The test adds the
CA to an in-memory connector; it never installs it in the system trust store.
The server certificate intentionally excludes IP addresses so hostname mismatch
verification can be tested. Certificates expire in 2126. Do not use them for
deployment.
