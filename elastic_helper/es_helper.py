import json
import datetime
import numpy as np
import pytz
import tzlocal
import pandas as pd
import logging
import collections

from cachetools import cached, LRUCache, TTLCache

@cached(cache=TTLCache(maxsize=1024, ttl=300))
def get_es_info(es):
    print('get_es_info')
    return es.info()


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        elif isinstance(o, datetime.time):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


def elastic_to_dataframe(es, index, query="*", start=None, end=None, sort=None, timestampfield="@timestamp", datecolumns=[], _source=[], size=None, scrollsize=5000):
    """Convert an elastic collection to a dataframe.

    Parameters:
    es -- The elastic connection object
    query -- (optional) The elastic query
    start -- (optional) The time range start if any
    end -- (optional) The time range start if any
    timestampfield -- (optional) The timestamp field used by the start and stop parameters
    datecolumns -- (optional) A collection of columns that must be converted to dates
    scrollsize -- (optional) The size of the scroll to use
    """

    logger = logging.getLogger()
    array = []
    recs = []

    try:
        version = int(es_info.get('version').get('number').split('.')[0])

        finalquery = {
            "_source": _source,
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": query,
                                "analyze_wildcard": True
                            }
                        }
                    ]
                }
            }
        }

        if start is not None:
            finalquery["query"]["bool"]["must"].append({
                "range": {

                }
            })

            finalquery["query"]["bool"]["must"][len(finalquery["query"]["bool"]["must"])-1]["range"][timestampfield] = {
                "gte": int(start.timestamp())*1000,
                "lte": int(end.timestamp())*1000,
                "format": "epoch_millis"
            }

        if sort is not None:
            finalquery["sort"] = sort

        print(finalquery)

        if size is not None and size < scrollsize:
            scrollsize = size

        res = es.search(index=index, size=scrollsize, scroll='2m', body=finalquery
                        )

        sid = res['_scroll_id']
        
        scroll_size = None
        if version < 7:
            scroll_size = res['hits']['total']
        else:
            scroll_size = res['hits']['total']['value']
            

        array = []
        for res2 in res["hits"]["hits"]:
            res2["_source"]["_id"] = res2["_id"]
            res2["_source"]["_index"] = res2["_index"]

            array.append(res2["_source"])

        recs = len(res['hits']['hits'])

        break_flag = False

        while (scroll_size > 0):
            res = es.scroll(scroll_id=sid, scroll='2m')
            sid = res['_scroll_id']
            scroll_size = len(res['hits']['hits'])
            logger.info("scroll size: " + str(scroll_size))
            logger.info("Next page:"+str(len(res['hits']['hits'])))
            recs += len(res['hits']['hits'])

            for res2 in res["hits"]["hits"]:
                if len(array) >= size:
                    break_flag = True
                    break

                res2["_source"]["_id"] = res2["_id"]
                res2["_source"]["_index"] = res2["_index"]
                array.append(res2["_source"])

            if break_flag:
                break

    except Exception as e:
        logger.error("Unable to load data.")
        logger.error(e)
    df = pd.DataFrame(array)

    if len(datecolumns) > 0 and len(df) > 0:
        containertimezone = pytz.timezone(tzlocal.get_localzone().zone)

        for col in datecolumns:
            if df[col].dtype == "int64":
                df[col] = pd.to_datetime(
                    df[col], unit='ms', utc=True).dt.tz_convert(containertimezone)
            else:
                df[col] = pd.to_datetime(
                    df[col], utc=True).dt.tz_convert(containertimezone)

    return df


def dataframe_to_elastic(es, df):
    """Converts a dataframe to an elastic collection to.
    The dataframe must have an "_index" column used to select the target index.
    Optionally an "_id" column can be used to specify the id of the record.
    Optionally an "_timestamp" column can be used to specify a "@timestamp column.

    Parameters:
    es -- The elastic connection object
    df -- The dataframe
    """

    logger = logging.getLogger(__name__)

    logger.info("LOADING DATA FRAME")
    logger.info("==================")

    if len([item for item, count in collections.Counter(df.columns).items() if count > 1]) > 0:
        logger.error("NNOOOOOOOOBBBB DUPLICATE COLUMN FOUND "*10)

    reserrors = []

    try:
        if len(df) == 0:
            logger.info('dataframe empty')
        else:
            logger.info("Loading data frame. Rows:" +
                        str(df.shape[1]) + " Cols:" + str(df.shape[0]))
            logger.info("Loading data frame")

        bulkbody = ""

        for index, row in df.iterrows():
            action = {}

            action["index"] = {"_index": row["_index"],
                               "_type": "doc"}
            if "_id" in row:
                action["index"]["_id"] = row["_id"]

            bulkbody += json.dumps(action, cls=DateTimeEncoder) + "\r\n"
            obj = {}

            for i in df.columns:

                if((i != "_index") and (i != "_timestamp")and (i != "_id")):
                    if not (type(row[i]) == str and row[i] == 'NaT') and \
                       not (type(row[i]) == pd._libs.tslibs.nattype.NaTType):
                        obj[i] = row[i]
                elif(i == "_timestamp"):
                    if type(row[i]) == int:
                        obj["@timestamp"] = int(row[i])
                    else:
                        obj["@timestamp"] = int(row[i].timestamp()*1000)

            bulkbody += json.dumps(obj, cls=DateTimeEncoder) + "\r\n"
            #print(json.dumps(obj, cls=DateTimeEncoder))

            if len(bulkbody) > 512000:
                logger.info("BULK READY:" + str(len(bulkbody)))
                # print(bulkbody)
                bulkres = es.bulk(bulkbody, request_timeout=30)
                logger.info("BULK DONE")
                currec = 0
                bulkbody = ""

                if(not(bulkres["errors"])):
                    logger.info("BULK done without errors.")
                else:
                    for item in bulkres["items"]:
                        if "error" in item["index"]:
                            # logger.info(item["index"]["error"])
                            reserrors.append(
                                {"error": item["index"]["error"], "id": item["index"]["_id"]})

        if len(bulkbody) > 0:
            logger.info("BULK READY FINAL:" + str(len(bulkbody)))
            bulkres = es.bulk(bulkbody)
            # print(bulkbody)
            logger.info("BULK DONE FINAL")

            if(not(bulkres["errors"])):
                logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        # logger.info(item["index"]["error"])
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})

        if len(reserrors) > 0:
            logger.info(reserrors)


    except:
        logger.error("Unable to store data in elastic", exc_info=True)

    return {
        'reserrors': reserrors
    }
