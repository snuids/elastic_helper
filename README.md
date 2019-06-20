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

```python
from elastic_helper import es_helper 

dataframe=es_helper.elastic_to_dataframe(es,index="docker_stats*",scrollsize=1000,datecolumns=["read"]
                                ,timestampfield="read"
                                ,start=datetime.datetime.now()-datetime.timedelta(hours=1)
                                ,end=datetime.datetime.now())                                                               
```

## Example Pandas To Elastic

```python
from elastic_helper import es_helper 

dataframe=es_helper.dataframe_to_elastic(es,my_df)                                                               
```

