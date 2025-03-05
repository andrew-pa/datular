# datular

A small key-value store implemented using the log-structured merge tree data structure.

## Interface
We support a few RPC operations via HTTP as the public interface:

- `GET /d/<key>`: gets the bytes stored under `<key>`, or 404s if it does not exist.
- `PUT /d/<key>`: puts the bytes passed in the body of the request under `<key>`, overwriting them if the key already exists.
- `DELETE /d/<key>`: deletes the key-value pair stored under `<key>`.
- `GET /health`: returns 200 if the service is up.
