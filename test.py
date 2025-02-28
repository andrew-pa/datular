#!/usr/bin/env python3
# /// script
# dependencies = [
#   "pytest", "requests"
# ]
# ///

"""
Integration Test Suite for Minimal Durable Key-Value Datastore

This test suite verifies the correctness, durability, and performance of a
minimal durable key-value datastore that exposes a simple HTTP API. The tests
cover basic CRUD operations, durability across process restarts, handling of
large values, and a high volume of keys. The datastore process is launched as a
separate process using the executable pointed to by the DAT_EXE_PATH environment
variable. Data is persisted in a temporary directory under /tmp, and cleanup is
performed after each test.

Environment Variables for the Datastore Service:
  - DAT_EXE_PATH: Path to the datastore executable (must be set in the test env).
  - DAT_LISTEN_ADDR: Address/port on which the datastore listens (default: 0.0.0.0:3333).
  - DAT_STORAGE_DIR: Directory used for persisting data (provided by the test).
  - DAT_MAX_IN_MEMORY_VALUES: Maximum number of in-memory key-value pairs (default: 1024).

Dependencies:
  - pytest
  - parameterized
  - requests

Usage:
  Run the tests with:
      pytest <this_script.py>
"""

import os
import subprocess
import time
import tempfile
import shutil

import pytest
import requests

# The service is assumed to listen on this URL. Although DAT_LISTEN_ADDR defaults to
# "0.0.0.0:3333", we connect via localhost.
SERVICE_URL = "http://127.0.0.1:3333"

enable_logging = True

def start_datastore(storage_dir, additional_env=None):
    """
    Starts the datastore service as a subprocess using the given storage directory.
    Optionally updates the environment with additional settings.

    Returns:
        subprocess.Popen: The process handle for the datastore service.
    """
    env = os.environ.copy()
    env["DAT_STORAGE_DIR"] = storage_dir
    if enable_logging:
        env["RUST_LOG"] = "debug"
    if additional_env:
        env.update(additional_env)

    exe_path = os.environ.get("DAT_EXE_PATH")
    if not exe_path:
        pytest.skip("DAT_EXE_PATH environment variable not set")
    proc = subprocess.Popen(
        [exe_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the service to be up by polling a non-existent key.
    for _ in range(10):
        try:
            r = requests.get(f"{SERVICE_URL}/d/nonexistent", timeout=1)
            if r.status_code in (404, 200):
                break
        except Exception:
            time.sleep(0.5)
    else:
        proc.kill()
        pytest.skip("Datastore service did not start in time")
    return proc

@pytest.fixture
def datastore_instance():
    """
    Pytest fixture that starts a datastore service instance with a temporary storage
    directory under /tmp. Yields the service URL for use in tests and ensures cleanup
    after the test finishes.
    """
    storage_dir = tempfile.mkdtemp(prefix="datastore_", dir="/tmp")
    proc = start_datastore(storage_dir)
    try:
        yield SERVICE_URL
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        shutil.rmtree(storage_dir)

# ---------------------------------------------------------------------------
# Helper functions to interact with the datastore HTTP API.
# Each function takes a base URL (from the fixture) to allow flexibility.
# ---------------------------------------------------------------------------

def put_key_url(base_url, key, value):
    """Stores the given value at the specified key."""
    full_url = f"{base_url}/d/{key}"
    r = requests.put(full_url, data=value)
    r.raise_for_status()

def get_key_url(base_url, key):
    """Retrieves the value stored at the specified key, or returns None if not found."""
    full_url = f"{base_url}/d/{key}"
    r = requests.get(full_url)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content

def delete_key_url(base_url, key):
    """Deletes the key from the datastore. Ignores deletion of non-existent keys."""
    full_url = f"{base_url}/d/{key}"
    r = requests.delete(full_url)
    if r.status_code not in (200, 204, 404):
        r.raise_for_status()

# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

def test_get_nonexistent(datastore_instance):
    """
    Test that performing a GET on a non-existent key returns 404 (i.e., None).
    """
    val = get_key_url(datastore_instance, "nonexistent")
    assert val is None

def test_put_and_get(datastore_instance):
    """
    Test that after a PUT operation, a GET returns the same value.
    """
    key = "testkey"
    value = b"testvalue"
    put_key_url(datastore_instance, key, value)
    retrieved = get_key_url(datastore_instance, key)
    assert retrieved == value

def test_delete(datastore_instance):
    """
    Test that DELETE correctly removes a key from the datastore.
    """
    key = "deletekey"
    value = b"todelete"
    put_key_url(datastore_instance, key, value)
    # Confirm that the key exists.
    assert get_key_url(datastore_instance, key) == value
    # Delete the key.
    delete_key_url(datastore_instance, key)
    # Confirm deletion.
    assert get_key_url(datastore_instance, key) is None

@pytest.mark.parametrize("label,size", [
    ("small", 10),
    ("medium", 1024),      # 1KB value
    ("large", 1024 * 1024) # 1MB value
])
def test_large_value(datastore_instance, label, size):
    """
    Test that the datastore correctly handles values of various sizes.
    """
    key = f"value_{label}"
    value = os.urandom(size)
    put_key_url(datastore_instance, key, value)
    retrieved = get_key_url(datastore_instance, key)
    assert retrieved == value

@pytest.mark.parametrize("label,num_keys", [
    ("num_keys_100", 100),
    ("num_keys_1500", 1500),  # Exceeds default in-memory threshold to force disk flush
])
def test_many_keys(datastore_instance, label, num_keys):
    """
    Test that a large number of keys can be stored and retrieved correctly.
    """
    stored_values = {}
    for i in range(num_keys):
        key = f"key_{i}"
        value = os.urandom(128)  # 128 bytes per key
        stored_values[key] = value
        put_key_url(datastore_instance, key, value)
    # Verify all keys.
    for key, expected in stored_values.items():
        retrieved = get_key_url(datastore_instance, key)
        assert retrieved == expected

def test_update_value(datastore_instance):
    """
    Test that updating the value for an existing key correctly replaces the old value.
    """
    key = "updatekey"
    initial_value = b"initial"
    updated_value = b"updated"
    put_key_url(datastore_instance, key, initial_value)
    assert get_key_url(datastore_instance, key) == initial_value
    # Update the key.
    put_key_url(datastore_instance, key, updated_value)
    assert get_key_url(datastore_instance, key) == updated_value

def test_sequence_operations(datastore_instance):
    """
    Test a sequence of operations:
      - Insert multiple keys.
      - Update a subset.
      - Delete the remainder.
    Verifies that the final datastore state is consistent.
    """
    keys = [f"seq_{i}" for i in range(50)]
    # Insert all keys with initial value.
    for key in keys:
        put_key_url(datastore_instance, key, b"v1")
    # Update the first half.
    for key in keys[:25]:
        put_key_url(datastore_instance, key, b"v2")
    # Delete the second half.
    for key in keys[25:]:
        delete_key_url(datastore_instance, key)
    # Validate updates.
    for key in keys[:25]:
        assert get_key_url(datastore_instance, key) == b"v2"
    for key in keys[25:]:
        assert get_key_url(datastore_instance, key) is None

def test_delete_nonexistent(datastore_instance):
    """
    Test that attempting to DELETE a non-existent key does not cause an error.
    """
    delete_key_url(datastore_instance, "nonexistent")

def test_durability_across_restarts():
    """
    Test datastore durability by:
      - Starting the datastore and inserting multiple keys.
      - Terminating the process.
      - Restarting the datastore with the same storage directory.
      - Verifying that the keys are persisted.
    """
    storage_dir = tempfile.mkdtemp(prefix="datastore_", dir="/tmp")
    proc = start_datastore(storage_dir)
    service_url = SERVICE_URL

    try:
        # Insert a set of key-value pairs.
        keys = {f"dur_{i}": os.urandom(64) for i in range(100)}
        for key, value in keys.items():
            put_key_url(service_url, key, value)
        # Allow time for potential disk flush.
        time.sleep(1)
    finally:
        proc.terminate()
        proc.wait(timeout=5)

    # Restart the datastore using the same storage directory.
    proc2 = start_datastore(storage_dir)
    service_url = SERVICE_URL
    try:
        for key, expected in keys.items():
            retrieved = get_key_url(service_url, key)
            assert retrieved == expected
    finally:
        proc2.terminate()
        proc2.wait(timeout=5)
        shutil.rmtree(storage_dir)

# ---------------------------------------------------------------------------
# Entry point for running tests directly.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, *sys.argv]))
