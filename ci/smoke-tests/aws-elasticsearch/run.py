#!/usr/bin/env python3

import argparse
import sys
from cfenv import AppEnv
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Service constants
SERVICE = "es"
REGION = "us-gov-west-1"

# Small example document to load
INDEX = "movies"
DOC_TYPE = "doc"
ID = "1"
DOCUMENT = {
    "title": "Black Panther",
    "director": "Ryan Coogler",
    "starring": [
        "Chadwick Boseman",
        "Michael B. Jordan",
        "Lupita Nyong'o",
        "Danai Gurira",
        "Martin Freeman",
        "Daniel Kaluuya",
        "Letitia Wright",
        "Winston Duke",
        "Angela Bassett",
        "Forest Whitaker",
        "Andy Serkis",
    ],
    "year": "2018",
}


def get_es_credentials(service_name):
    """
    Get elasticsearch credentials via the service name.
    """
    env = AppEnv()
    service = env.get_service(name=service_name)
    return service.credentials


def create_client(service_name):
    """
    Create an elasticsearch client from the service name
    """
    credentials = get_es_credentials(service_name)
    host = credentials["host"]
    # port = credentials["port"]
    access_key = credentials["access_key"]
    secret_key = credentials["secret_key"]

    aws_auth = AWS4Auth(access_key, secret_key, REGION, SERVICE)

    client = Elasticsearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    return client


class ESSmokeTester:
    def __init__(self, service_name):
        self.client = create_client(service_name)
        self.sample_index_options = {
            "index": INDEX,
            "doc_type": DOC_TYPE,
            "id": ID,
            "body": DOCUMENT,
        }

    def index_and_get(self, options=dict()):
        """
        Use an elasticsearch client to create an index then
        print the results of getting that index.
        """
        client = self.client

        client.index(**options)

        result = client.get(
            index=options["index"], doc_type=options["doc_type"], id=options["id"]
        )

        return result

    def delete_index(self, index_name):
        result = self.client.delete(INDEX, ID)
        return result

    def run(self):
        """
        Run the index_and_get method with test data
        Returns result dict
        """
        try:
            results = self.index_and_get(self.sample_index_options)
            return results
        except Exception as e:
            print(e)
            sys.exit(1)

    def test_expected(self, results):
        return (
            results["_index"] == INDEX
            and results["_type"] == DOC_TYPE
            and results["_id"] == ID
            and results["_source"] == DOCUMENT
        )


parser = argparse.ArgumentParser(
    description="Smoke tests for aws-elasticsearch service",
)


parser.add_argument(
    "-s",
    "--service-name",
    dest="service_name",
    type=str,
    help="The name of the service",
    required=True,
)


if __name__ == "__main__":
    args = parser.parse_args()
    service_name = args.service_name
    tester = ESSmokeTester(service_name)
    results = tester.run()
    isExpected = tester.test_expected(results)

    if isExpected is not True:
        print("Results did not match.")
        print("Results")
        print(results)
        sys.exit(1)
