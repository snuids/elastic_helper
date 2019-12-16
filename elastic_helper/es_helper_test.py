import os
import time
import json
import time
import datetime
import requests
import unittest
import logging
import urllib3
import pandas as pd
import elasticsearch
from datetime import datetime
from datetime import timedelta
from elasticsearch import helpers
from elastic_helper import es_helper

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

# git tag 1.0.1 -m "PyPi tag"
# git push --tags origin master
# python setup.py sdist
# twine upload dist/*

# python -m unittest  elastic_helper.es_helper_test

logger = None


server = os.environ["ES_HOST_TEST"]
user = os.environ["ES_USER_TEST"]
pwd = os.environ["ES_PWD_TEST"]


class TestClient(unittest.TestCase):
    """
    Test helper
    """

    def test_elastic_to_panda(self):
        """
        Send Receive
        """

        global server, pwd, user

        print("==> test_elastic_to_panda")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                user, pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", scrollsize=1000, datecolumns=[
                                                  "read"], timestampfield="read", start=datetime.now()-timedelta(hours=1), end=datetime.now())
            res2["read"].describe()
            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue(len(res2) > 0)
        finally:
            pass

    def test_size(self):

        global server, pwd, user

        print("==> test_size")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                user, pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", size=10, timestampfield="read",
                                                  start=datetime.now()-timedelta(hours=1), end=datetime.now())

            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue((len(res2) >= 0) and (len(res2) <= 10))
        finally:
            pass

    def test_size_2(self):

        global server, pwd, user

        print("==> test_size_2")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                user, pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", size=345, scrollsize=100, timestampfield="read",
                                                  start=datetime.now()-timedelta(hours=10), end=datetime.now())

            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue((len(res2) == 345))
        finally:
            pass

    def test_size_3(self):

        global server, pwd, user

        print("==> test_size_3")
        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                user, pwd), use_ssl=True, verify_certs=False)

            print(es.info())
            res2 = es_helper.elastic_to_dataframe(es, index="docker_stats*", size=110, scrollsize=300, timestampfield="read",
                                                  start=datetime.now()-timedelta(hours=10), end=datetime.now())

            print(len(res2))

            self.assertTrue(res2 is not None)
            self.assertTrue((len(res2) == 110))
        finally:
            pass

    def test_date_cols(self):
        global server, pwd, user

        print("==> test_date_cols")

        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                user, pwd), use_ssl=True, verify_certs=False)

            print(es.info())

            try:
                es.indices.delete('test_date_cols')
            except elasticsearch.NotFoundError:
                pass
            
            doc = {
                'attr1': 'test'
            }
            es.index(index="test_date_cols", id="t1", doc_type='_doc', body=doc)
            
            time.sleep(1)
            res = es_helper.elastic_to_dataframe(es, index="test_date_cols", datecolumns=["date1"])

            print(len(res))
            print(res.columns)

            self.assertTrue("date1" in res.columns)

            doc = {
                'attr1': 'test',
                'date1': datetime.now()
            }

            es.index(index="test_date_cols", id="t2", doc_type='_doc', body=doc)
            
            time.sleep(1)
            res = es_helper.elastic_to_dataframe(es, index="test_date_cols", datecolumns=["date1"])
            
            self.assertTrue("date1" in res.columns)

            doc = {
                'attr1': 'test',
                'date2': datetime.now()
            }

            es.index(index="test_date_cols", id="t3", doc_type='_doc', body=doc)
            
            time.sleep(1)
            res = es_helper.elastic_to_dataframe(es, index="test_date_cols", datecolumns=["date1", "date2"])
            
            self.assertTrue("date1" in res.columns)
            self.assertTrue("date2" in res.columns)

            es.indices.delete('test_date_cols')
        finally:
            pass
    
    def test_empty_attr(self):
        global server, pwd, user

        print("==> test_empty_attr")

        try:
            host_params1 = {'host': server,
                            'port': 9200, 'use_ssl': True}  # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=(
                user, pwd), use_ssl=True, verify_certs=False)

            print(es.info())

            try:
                es.indices.delete('test_empty_attr')
            except elasticsearch.NotFoundError:
                pass

            arr = [
                {
                    '_id': 't1',
                    'attr1': 'test'
                },
                {
                    '_id': 't2',
                    'attr2': 'test'
                },
                {
                    '_id': 't3',
                    'attr3': 'test'
                },
            ]

            df = pd.DataFrame(arr)
            
            INDEX_NAME = 'test_empty_attr'
            
            df['_index'] = INDEX_NAME
            
            es_helper.dataframe_to_elastic(es, df)
            time.sleep(1)

            rec = es.get(index = INDEX_NAME, id = 't1', doc_type = 'doc')

            self.assertTrue("attr1" in rec['_source'])
            self.assertTrue("attr2" not in rec['_source'])
            self.assertTrue("attr3" not in rec['_source'])
        finally:
            pass