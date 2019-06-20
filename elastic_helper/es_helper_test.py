import os
import time
import json
import datetime
import requests
import unittest
import logging
import urllib3
from elastic_helper import es_helper

from elasticsearch import helpers
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

# git tag 1.0.1 -m "PyPi tag"
# git push --tags origin master
# python setup.py sdist
# twine upload dist/*

# python -m unittest  elastic_helper.es_helper_test

logger = None

server = os.environ["ES_HOST_TEST"]
pwd = os.environ["ES_PWD_TEST"]


class TestClient(unittest.TestCase):
    """
    Test helper
    """

    def test_elastic_to_panda(self):
        """
        Send Receive
        """

        global server, pwd

        print("==> test_elastic_to_panda")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                "user", pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", scrollsize=1000, datecolumns=[
                                                  "read"], timestampfield="read", start=datetime.datetime.now()-datetime.timedelta(hours=1), end=datetime.datetime.now())
            res2["read"].describe()
            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue(len(res2) > 0)
        finally:
            pass

    def test_size(self):

        global server, pwd

        print("==> test_size")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                "user", pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", size=10, timestampfield="read",
                                                  start=datetime.datetime.now()-datetime.timedelta(hours=1), end=datetime.datetime.now())

            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue((len(res2) >= 0) and (len(res2) <= 10))
        finally:
            pass

    def test_size_2(self):

        global server, pwd

        print("==> test_size_2")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                "user", pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", size=345, scrollsize=100, timestampfield="read",
                                                  start=datetime.datetime.now()-datetime.timedelta(hours=10), end=datetime.datetime.now())

            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue((len(res2) == 345))
        finally:
            pass

    def test_size_3(self):

        global server, pwd

        print("==> test_size_3")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                "user", pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", size=110, scrollsize=300, timestampfield="read",
                                                  start=datetime.datetime.now()-datetime.timedelta(hours=10), end=datetime.datetime.now())

            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue((len(res2) == 110))
        finally:
            pass
