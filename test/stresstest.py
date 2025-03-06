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
import os
from locust import HttpUser, task, between

class DatastoreUser(HttpUser):
    """
    Locust user class to simulate interactions with the key-value datastore.
    """
    # Wait time between tasks to simulate real-world usage (in seconds)
    wait_time = between(0.3, 3)

    def on_start(self):
        self.old_key = self.generate_key()
        self.old_val = self.generate_value()
        self.new_key = self.generate_key()
        self.new_val = self.generate_value()
        self.put_key(self.old_key, self.old_val)
        self.put_key(self.new_key, self.new_val)

    def generate_key(self):
        return uuid.uuid4().hex

    def generate_value(self, size=1024):
        # Repeat the uuid hex string to reach desired length.
        return os.urandom(size)

    def put_key(self, key, value):
        with self.client.put(f"/d/{key}", data=value, catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"PUT failed for key {key} with status {response.status_code}")
            else:
                response.success()
        return response

    def get_key(self, key):
        with self.client.get(f"/d/{key}", catch_response=True) as response:
            if response.status_code not in (200, 404):
                response.failure(f"GET failed for key {key} with status {response.status_code}")
            else:
                response.success()
        return response

    def delete_key(self, key):
        with self.client.delete(f"/d/{key}", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"DELETE failed for key {key} with status {response.status_code}")
            else:
                response.success()
        return response

    @task(100)
    def write(self):
        self.put_key(self.generate_key(), self.generate_value())

    @task(10)
    def update_new_key(self):
        self.new_val = self.generate_value()
        self.put_key(self.new_key, self.new_val)

    @task(1)
    def read_old_key(self):
        with self.client.get(f"/d/{self.old_key}", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"GET failed for key {self.old_key} with status {response.status_code}")
            elif response.text != self.old_val:
                response.failure(f"GET returned wrong value for key {self.old_key}")
            else:
                response.success()

    @task(5)
    def read_new_key(self):
        with self.client.get(f"/d/{self.new_key}", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"GET failed for key {self.new_key} with status {response.status_code}")
            elif response.text != self.new_val:
                response.failure(f"GET returned wrong value for key {self.new_key}")
            else:
                response.success()



