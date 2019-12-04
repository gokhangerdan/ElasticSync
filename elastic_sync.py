import pyodbc
import pandas as pd
from elasticsearch import Elasticsearch
import uuid
import base64
from datetime import datetime
import numpy as np


class MSSQLConnector:
    __conn = None
    database = None

    def __init__(self, server, database, user, password):
        conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                              "Server="+server+";"
                              "Database="+database+";"
                              "uid="+user+";pwd="+password)
        self.__conn = conn
        self.database = database

    def run_sql_query(self, query):
        return pd.read_sql_query(query, self.__conn)


class ElasticConnector:
    __conn = None

    def __init__(self, host, port):
        conn = Elasticsearch([{
            'host': host,
            'port': port
        }])
        self.__conn = conn

    def elastic_sync(self, db, fill_null={
        "datetime64": datetime(1678, 1, 1, 0, 0),
        "object": ""
    }):
        table_names = db.run_sql_query(
            """
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE'
            """
        )["TABLE_NAME"].tolist()

        for table_name in table_names:
            next_table = db.run_sql_query(
                "SELECT * FROM ["+table_name+"]"
            )

            contains_null = next_table.columns[next_table.isnull().any()]
            for column_name in contains_null:
                if np.issubdtype(next_table[column_name].dtype, np.datetime64):
                    next_table[column_name] = next_table[column_name].fillna(
                        fill_null["datetime64"]
                    )
                if np.issubdtype(next_table[column_name].dtype, np.object):
                    next_table[column_name] = next_table[column_name].fillna(
                        fill_null["object"]
                    )

            records_json = next_table.to_dict(orient='records')

            for record in records_json:
                for key in record.keys():
                    if type(record[key]) == bytes:
                        record[key] = base64.b64encode(record[key])
                try:
                    res = self.__conn.index(
                        index="_".join([
                            db.database.lower().replace(" ", "_"),
                            table_name.lower().replace(" ", "_")
                        ]),
                        id=str(uuid.uuid4()),
                        body=record
                    )

                    print(res)
                except Exception as e:
                    print("\n\n\n\n\n")
                    print(e)
                    return record


if __name__ == '__main__':
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

    record = es.elastic_sync(db)

    df = pd.DataFrame([record])

    print(df.info())
