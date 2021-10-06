#!/usr/bin/env python3

import argparse
import os
from cfenv import AppEnv
import redis


def redis_client(service_name):
    """
    Connecting to a Redis service
    """
    config = dict(host=service_name, port=6379, password="", ssl_cert_reqs=None)

    if service_name is None:
        return None

    try:
        if "VCAP_SERVICES" in os.environ:
            env = AppEnv()
            service = env.get_service(name=service_name)
            credentials = service.credentials

            config["host"] = credentials["host"]
            config["ssl"] = True
            config["port"] = int(credentials["port"])
            config["password"] = credentials["password"]

        client = redis.Redis(**config)
    except redis.ConnectionError:
        client = None

    return client


class RedisSmokeTester:
    """
    This is a smoke tester for redis services.
    """

    def __init__(self, service_name, records_per_seed=10):
        self.client = redis_client(service_name)
        self.records_per_seed = records_per_seed

    def get_expected(self):
        records_per_seed = self.records_per_seed

        return {
            "counts": {
                "keys_str": records_per_seed,
                "keys_num": records_per_seed,
                "keys_str_ttl": records_per_seed,
                "keys_num_ttl": records_per_seed,
            }
        }

    def load_data(self):
        """
        Method to load data into redis
        """
        records_per_seed = self.records_per_seed

        for x in range(records_per_seed):
            self.client.set(f"key-str-{x}", f"value={x}")

        for x in range(records_per_seed):
            self.client.set(f"key-num-{x}", x * 2)

        for x in range(records_per_seed):
            key = f"key-ttl-str-{x}"
            self.client.set(key, f"value-with-ttl={x}")
            self.client.expire(key, 1000)

        for x in range(records_per_seed):
            key = f"key-ttl-num-{x}"
            self.client.set(key, x * 2)
            self.client.expire(key, 1000)

    def get_data(self):
        """
        Method to get loaded data from load_data method
        """
        keys_str = self.client.keys("key-str*")
        keys_num = self.client.keys("key-num*")
        keys_str_ttl = self.client.keys("key-ttl-str*")
        keys_num_ttl = self.client.keys("key-ttl-num*")

        return {
            "counts": {
                "keys_str": len(keys_str),
                "keys_num": len(keys_num),
                "keys_str_ttl": len(keys_str_ttl),
                "keys_num_ttl": len(keys_num_ttl),
            }
        }

    def flush(self):
        """
        flush redis to clean up after test
        """
        self.client.flushall()

    def run(self):
        """
        Run the load, get, and flush methods
        Returns result dict
        """
        try:
            self.load_data()
            results = self.get_data()
            self.flush()

            return results
        except Exception as e:
            print(e)
            os.sys.exit(1)


parser = argparse.ArgumentParser(
    description="Smoke tests for aws-elasticache-redis service",
)


parser.add_argument(
    "-s",
    "--service-name",
    dest="service_name",
    type=str,
    help="The name of the Redis service",
    required=True,
)


if __name__ == "__main__":
    args = parser.parse_args()
    service_name = args.service_name
    redis_tester = RedisSmokeTester(service_name)
    results = redis_tester.run()
    expected = redis_tester.get_expected()

    isExpected = results == expected

    if isExpected is not True:
        print("Results did not match.")
        print("Results")
        print(results)
        print("Expected")
        print(expected)
        os.sys.exit(1)
