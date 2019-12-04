UNDER DEVELOPMENT

# ElasticSync
Sync database from sql server to elasticsearch.

# Usage
```python
from elastic_sync import MSSQLConnector, ElasticConnector

db = MSSQLConnector(
    server="localhost",
    database="Northwind",
    user="sa",
    password="yourStrong(!)Password"
)

es = ElasticConnector(
    host="localhost",
    port=9200
)
```
