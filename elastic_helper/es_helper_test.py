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



#git tag 1.0.1 -m "PyPi tag"
#git push --tags origin master
#python setup.py sdist
#twine upload dist/*


logger=None

class TestClient(unittest.TestCase):
    """
    Test helper
    """
   

    def test_elastic_to_panda(self):
        """
        Send Receive
        """
        logger = logging.getLogger()  
        logger.info("==> test_elastic_to_panda")
        try:            
            host_params1 = {'host':'********', 'port':9200, 'use_ssl':True} # RPINUM

            es = ES([host_params1], connection_class=RC, http_auth=("user","********"), use_ssl=True,verify_certs=False)

            print(es.info())
            res2=es_helper.elastic_to_dataframe(es,index="*******",scrollsize=1000,datecolumns=["read"]
                                ,timestampfield="read"
                                ,start=datetime.datetime.now()-datetime.timedelta(hours=1)
                                ,end=datetime.datetime.now())
            res2["read"].describe()
            print(res2)
            #logger.info(conn.listener.received)
            #self.assertEqual("/queue/QTEST1" in conn.listener.received, True)
        finally:
            pass