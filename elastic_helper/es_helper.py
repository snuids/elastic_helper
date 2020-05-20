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
    logger = logging.getLogger()
    logger.debug('get_es_info')
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
    sort -- (optional) The column we want to sort on
    timestampfield -- (optional) The timestamp field used by the start and stop parameters
    datecolumns -- (optional) A collection of columns that must be converted to dates
    _source -- (optional) columns we want to retrieve
    size -- (optional) The max number of recrods we want to retrieve
    scrollsize -- (optional) The size of the scroll to use
    """

    logger = logging.getLogger()
    array = []
    recs = []
    scroll_ids=[]


    version = int(get_es_info(es).get('version').get('number').split('.')[0])

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

    logger.debug(finalquery)

    if size is not None and size < scrollsize:
        scrollsize = size

    res = es.search(index=index, size=scrollsize, scroll='1m', body=finalquery
                    )

    sid = res['_scroll_id']
    scroll_ids.append(sid)
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
        scroll_ids.append(sid)
        scroll_size = len(res['hits']['hits'])
        logger.info("scroll size: " + str(scroll_size))
        logger.info("Next page:"+str(len(res['hits']['hits'])))
        recs += len(res['hits']['hits'])

        for res2 in res["hits"]["hits"]:
            if size is not None and len(array) >= size:
                break_flag = True
                break

            res2["_source"]["_id"] = res2["_id"]
            res2["_source"]["_index"] = res2["_index"]
            array.append(res2["_source"])

        if break_flag:
            break

    df = pd.DataFrame(array)

    if len(datecolumns) > 0 and len(df) > 0:
        containertimezone = pytz.timezone(tzlocal.get_localzone().zone)

        for col in datecolumns:
            if col not in df.columns:
                df[col] = None
            else:
                if df[col].dtype == "int64":
                    df[col] = pd.to_datetime(
                        df[col], unit='ms', utc=True).dt.tz_convert(containertimezone)
                else:
                    df[col] = pd.to_datetime(
                        df[col], utc=True).dt.tz_convert(containertimezone)
                    
    es.clear_scroll(body={'scroll_id': scroll_ids})
    return df


def dataframe_to_elastic(es, df, doc_type='doc'):
    """Converts a dataframe to an elastic collection to.
    The dataframe must have an "_index" column used to select the target index.
    Optionally an "_id" column can be used to specify the id of the record.
    Optionally an "_timestamp" column can be used to specify a "@timestamp column.

    Parameters:
    es -- The elastic connection object
    df -- The dataframe
    """

    logger = logging.getLogger(__name__)

    logger.debug("LOADING DATA FRAME")
    logger.debug("==================")

    version = int(get_es_info(es).get('version').get('number').split('.')[0])


    if len([item for item, count in collections.Counter(df.columns).items() if count > 1]) > 0:
        logger.error("NNOOOOOOOOBBBB DUPLICATE COLUMN FOUND "*10)
        raise Exception('Duplicate column in DataFrame')

    reserrors = []

    if len(df) == 0:
        logger.warning('dataframe empty')
    else:
        logger.debug("Loading data frame. Rows:" +
                    str(df.shape[1]) + " Cols:" + str(df.shape[0]))

    bulkbody = ""

    df_json = json.loads(df.to_json(orient='records'))
    
    action = {}
    action["index"] = {"_type": doc_type}

    flag_unique_index = False

    if len(df['_index'].unique() == 1):
        flag_unique_index = True
        
        action["index"]["_index"] = df.iloc[0]["_index"]
        
    for row in df_json:
        if "_id" in row:
            action["index"]["_id"] = row["_id"]
            
        if not flag_unique_index:
            action["index"]["_index"] = row["_index"]
            
        bulkbody += json.dumps(action, cls=DateTimeEncoder) + "\r\n"
        bulkbody += json.dumps({k:v for k,v in row.items() if k!='_id' and k!='_index' and v is not None}, 
                            cls=DateTimeEncoder) + "\r\n"


        if len(bulkbody) > 512000:
            logger.debug("BULK READY:" + str(len(bulkbody)))
            # print(bulkbody)
            bulkres = es.bulk(bulkbody, request_timeout=30)
            logger.debug("BULK DONE")
            bulkbody = ""

            if(not(bulkres["errors"])):
                logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})

    if len(bulkbody) > 0:
        logger.debug("BULK READY FINAL:" + str(len(bulkbody)))
        bulkres = es.bulk(bulkbody)
        logger.debug("BULK DONE FINAL")

        if(not(bulkres["errors"])):
            logger.info("BULK done without errors.")
        else:
            for item in bulkres["items"]:
                if "error" in item["index"]:
                    reserrors.append(
                        {"error": item["index"]["error"], "id": item["index"]["_id"]})

    if len(reserrors) > 0:
        logger.warning(reserrors)



    return {
        'reserrors': reserrors
    }
