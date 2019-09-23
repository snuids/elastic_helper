# elastic_helper
Two simple functions
* One used to convert an elastic search collection into a dataframe. See the code for the various parameters.
* One used to convert a dataframe into an elastic search collection


## Installation

```sh
pip install elastic-helper
```


the pypi page (https://pypi.org/project/elastic-helper/):

## Example Elastic To Pandas

### Parameters

* **es** -- The elastic connection object
* **index** -- The elastic index
* **query** -- (optional) The elastic query in lucene format Example: "module: *"
* **start** -- (optional) The time range start if any
* **end** -- (optional) The time range start if any
* **timestampfield** -- (optional) The timestamp field used by the start and stop parameters
* **datecolumns** -- (optional) A collection of columns that must be converted to dates
* **scrollsize** -- (optional) The size of the scroll to use
* **size** -- (optional) The maximum number of records to retrieve
* **_source** -- (optional) The fields to retrieve

```python
from elastic_helper import es_helper 

dataframe=es_helper.elastic_to_dataframe(es,index="docker_stats*"
                                ,_source=['read', 'cpu_percent', 'name']
                                ,datecolumns=["read"]
                                ,timestampfield="read"
                                ,start=datetime.now()-timedelta(hours=1)
                                ,end=datetime.now())                                                               
```

## Example Pandas To Elastic

* Use an **_index** column in the dataframe to specify the target index
* Use an **_id** column in the dataframe to specify the id

```python
from elastic_helper import es_helper 

es_helper.dataframe_to_elastic(es,my_df)                                                               
```

