"""
Datastore Load Test Suite using Locust

This script is a load testing suite for a minimal durable key-value datastore.
It tests basic operations (PUT, GET, DELETE) for correctness under load,
simulates real-world concurrent usage, and helps determine the maximum
number of key-value pairs before service degradation.

Datastore API:
  - GET /d/{key}: returns the value stored at key (or 404 if not found)
  - PUT /d/{key}: stores the value in the HTTP request body at key
  - DELETE /d/{key}: deletes the key from the datastore

Usage:
    locust -f datastore_load_test.py --host=http://your-datastore-host

The Locust dashboard will provide graphs and metrics during the test.
"""

import uuid
import random
from locust import HttpUser, task, between

class DatastoreUser(HttpUser):
    """
    Locust user class to simulate interactions with the key-value datastore.
    """
    # Wait time between tasks to simulate real-world usage (in seconds)
    wait_time = between(0.3, 3)

    def on_start(self):
        """
        Called when a simulated user starts.
        Initializes a local store for keys created by this user.
        """
        self.keys = []  # List of tuples (key, value) for keys created by this user
        self.pending_key_groups = []

    def generate_key(self):
        """
        Generates a random URL-safe key.
        """
        return uuid.uuid4().hex

    def generate_value(self, size=32):
        """
        Generates a random hexadecimal string of approximate length 'size'.
        """
        # Repeat the uuid hex string to reach desired length.
        base = uuid.uuid4().hex
        return (base * ((size // len(base)) + 1))[:size]

    def put_key(self, key, value):
        """
        Helper method to perform a PUT request.
        """
        with self.client.put(f"/d/{key}", data=value, catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"PUT failed for key {key} with status {response.status_code}")
            else:
                response.success()
        return response

    def get_key(self, key):
        """
        Helper method to perform a GET request.
        """
        with self.client.get(f"/d/{key}", catch_response=True) as response:
            if response.status_code not in (200, 404):
                response.failure(f"GET failed for key {key} with status {response.status_code}")
            else:
                response.success()
        return response

    def delete_key(self, key):
        """
        Helper method to perform a DELETE request.
        """
        with self.client.delete(f"/d/{key}", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"DELETE failed for key {key} with status {response.status_code}")
            else:
                response.success()
        return response

    @task(15)
    def test_put_get_delete_cycle(self):
        """
        Test a full cycle: PUT a key, GET to verify correctness,
        DELETE the key, and ensure that a subsequent GET returns 404.
        """
        key = self.generate_key()
        value = self.generate_value()

        # PUT operation
        self.put_key(key, value)

        # GET operation: verify that the stored value matches
        with self.client.get(f"/d/{key}", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"GET failed for key {key} with status {response.status_code}")
            elif response.text != value:
                response.failure(f"GET returned wrong value for key {key}. Expected {value}, got {response.text}")
            else:
                response.success()

        # DELETE operation
        self.delete_key(key)

        # GET again: should return 404
        with self.client.get(f"/d/{key}", catch_response=True) as response:
            if response.status_code != 404:
                response.failure(f"Expected 404 after DELETE for key {key}, got {response.status_code}")
            else:
                response.success()

    @task(20)
    def test_concurrent_puts_and_gets(self):
        """
        Simulate concurrent creation of many keys followed by immediate verification.
        This helps stress-test the datastore with rapid and multiple operations.
        """
        keys_created = []
        # Create several key-value pairs in quick succession.
        for _ in range(256):
            key = self.generate_key()
            value = self.generate_value()
            self.put_key(key, value)
            keys_created.append((key, value))

        random.shuffle(keys_created)

        # Verify that each key returns the expected value.
        for key, value in keys_created:
            with self.client.get(f"/d/{key}", catch_response=True) as response:
                if response.status_code != 200:
                    response.failure(f"Concurrent GET failed for key {key} with status {response.status_code}")
                elif response.text != value:
                    response.failure(f"Concurrent GET returned wrong value for key {key}")
                else:
                    response.success()

        # Clean up: delete all keys created.
        for key, _ in keys_created:
            self.delete_key(key)

    @task(100)
    def test_random_operations(self):
        """
        Perform random operations (PUT, GET, DELETE) to mimic real-world usage patterns.
        - For PUT: create a new key-value pair and store it locally.
        - For GET: retrieve a random key from those created earlier.
        - For DELETE: remove a random key and verify its removal.
        """
        operation = random.choice(["put", "get", "delete"])

        if operation == "put":
            key = self.generate_key()
            value = self.generate_value()
            self.put_key(key, value)
            self.keys.append((key, value))
        elif operation == "get":
            # Only perform GET if there is at least one key stored
            if self.keys:
                key, value = random.choice(self.keys)
                with self.client.get(f"/d/{key}", catch_response=True) as response:
                    if response.status_code != 200:
                        response.failure(f"Random GET failed for key {key} with status {response.status_code}")
                    elif response.text != value:
                        response.failure(f"Random GET returned wrong value for key {key}")
                    else:
                        response.success()
        elif operation == "delete":
            # Only perform DELETE if there is at least one key stored
            if self.keys:
                idx = random.randrange(len(self.keys))
                key, value = self.keys.pop(idx)
                self.delete_key(key)
                # Verify deletion: GET should return 404
                with self.client.get(f"/d/{key}", catch_response=True) as response:
                    if response.status_code != 404:
                        response.failure(f"Random DELETE did not remove key {key} properly")
                    else:
                        response.success()

    @task(50)
    def test_lots_of_puts(self):
        keys_created = []
        for _ in range(256):
            key = self.generate_key()
            value = self.generate_value()
            self.put_key(key, value)
            keys_created.append((key, value))
        random.shuffle(keys_created)
        self.pending_key_groups.append(keys_created)

    @task(50)
    def test_lots_of_gets(self):
        if len(self.pending_key_groups) == 0:
            return
        keys = random.choice(self.pending_key_groups)
        for key, value in keys:
            with self.client.get(f"/d/{key}", catch_response=True) as response:
                if response.status_code != 200:
                    response.failure(f"GET failed for key {key} with status {response.status_code}")
                elif response.text != value:
                    response.failure(f"GET returned wrong value for key {key}")
                else:
                    response.success()

    @task(20)
    def test_lots_of_deletes(self):
        if len(self.pending_key_groups) == 0:
            return
        idx = random.randrange(len(self.pending_key_groups))
        keys = self.pending_key_groups.pop(idx)
        for key, _ in keys:
            self.delete_key(key)


