# Databricks notebook source
# MAGIC %pip install elasticsearch
# MAGIC %pip install azure-storage-blob
# MAGIC import pandas as pd
# MAGIC from elasticsearch import Elasticsearch, helpers
# MAGIC from azure.storage.blob import BlobServiceClient
# MAGIC import json

# COMMAND ----------

# Establish connection to Elasticsearch
key = dbutils.secrets.get('databricksscope', 'elastic-search-key')
cloud_id = dbutils.secrets.get('databricksscope', 'elastic-search-cloudid')
connection = Elasticsearch(cloud_id=cloud_id, api_key=key)

# COMMAND ----------

# Retrieve records from Elasticsearch
index = 'lhh-stories'
records = list(helpers.scan(client=connection, index=index, preserve_order=True))
print(f'got {len(records)} records from lhh-stories')

# COMMAND ----------

# Flatten JSON 
def flatten(df):
    json_struct = json.loads(df.to_json(orient="records"))
    flattened_df = pd.json_normalize(json_struct)
    return flattened_df


# List checker
def has_list(x):
    def list_exists(x):
        return any(isinstance(i, list) for i in x)
    cols = df.apply(list_exists)
    cols_list = df.columns[cols].tolist()
    return cols_list


# Explode List object
def explode(df, cols):
    for col in cols:
        df = df.explode(col)

    exploded_df = df
    return exploded_df


# COMMAND ----------

import numpy as np
# Convert list of records into Dataframe
df = pd.DataFrame(records)

df = flatten(df)
list_of_columns_with_list_dtypes = has_list(df)
list_length = len(list_of_columns_with_list_dtypes)
while len(list_of_columns_with_list_dtypes) > 0:
    df = explode(df, list_of_columns_with_list_dtypes)
    df = flatten(df)
    list_of_columns_with_list_dtypes = has_list(df)

# df['_source.duration'] = df['_source.duration'].replace([np.inf, -np.inf, np.nan], 0)

df['RoundedDuration'] = df['_source.duration'].round(0).astype('Int64')
df['ExternalAssetEmbed'] = 'filler'
df['IsHidden'] = 'False'
df['DurationConverted'] = pd.to_datetime(df["_source.duration"], unit='s').dt.strftime("%H:%M:%S")
df['URLAnchorCombined'] = df['_id'] + df['_source.mentions.anchor']
df.reset_index()

df

# COMMAND ----------


spark_df = spark.createDataFrame(df)
display(spark_df)

# COMMAND ----------

server_name = "jdbc:sqlserver://training-ss.database.windows.net"
database_name = "training-db"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "mytable"
username = "matthew.perkins@stormid.com"
password = "Casio3298!?" # Please specify password here

try:
  spark_df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()
except ValueError as error :
    print("Connector write failed", error)

# COMMAND ----------

spark_df.createOrReplaceTempView("spark_df_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spark_df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS stories_test
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://delta@trainingsamp.dfs.core.windows.net/silver'
# MAGIC AS SELECT * FROM spark_df_table

# COMMAND ----------

# to test when access to ADLS is set up
spark_df = spark.read.format('json')
    .option('inferSchema', 'true')
    .load('PATH')
