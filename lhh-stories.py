# Databricks notebook source
# Install libraries
%pip install elasticsearch
%pip install azure-storage-blob
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from azure.storage.blob import BlobServiceClient
import json
import pyspark.sql.functions as F

# COMMAND ----------

# Create widget to switch between topcics, i.e. stories, areas, places, etc. May decide this is not the best approach, and each topic is to be handled by a specific notebook
dbutils.widgets.dropdown("topic", 'lhh-stories', ['lhh-stories', 'lhh-area-by-numbers', 'lhh-areas', 'lhh-grant-aided-places', 'lhh-historic-places', 'lhh-images', 'lhh-place-names'])


# COMMAND ----------

# Mount to ADLS - this mounting method is not suitable for production, Service Principle method must be used.
# account_key = dbutils.secrets.get('databricksscope', 'mount-sas-account-key')
# account_token = dbutils.secrets.get('databricksscope', 'mount-sas-account-token')
# dbutils.fs.mount(
#   source = "wasbs://training@trainingsamp.blob.core.windows.net",
#   mount_point = "/mnt/trainingsamp/training/",
#   extra_configs = {account_key:account_token}
# )

# dbutils.fs.unmount("/mnt/trainingsamp/training/")

# COMMAND ----------

# Establish connection to Elasticsearch using credentials held in key vault backed secret scope.
key = dbutils.secrets.get('databricksscope', 'elastic-search-key')
cloud_id = dbutils.secrets.get('databricksscope', 'elastic-search-cloudid')
connection = Elasticsearch(cloud_id=cloud_id, api_key=key)


# COMMAND ----------

# Retrieve records from Elastic search and convert into Dataframe
topic_name = dbutils.widgets.get("topic")
records = helpers.scan(client=connection, index=topic_name, preserve_order=True)
list_records = list(records)
raw_spark_df = spark.createDataFrame(list_records)
pandas_df = pd.DataFrame(list_records)


# COMMAND ----------

# Persist bronze table to Delta lake
raw_spark_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"/mnt/trainingsamp/training/{topic_name}/bronze")

# COMMAND ----------

# Define function to unpack the nested structure of the dataframe.
def unpack(df):
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

    df = flatten(df)
    
    list_of_columns_with_list_dtypes = has_list(df)

    list_length = len(list_of_columns_with_list_dtypes)

    while len(list_of_columns_with_list_dtypes) > 0:

        df = explode(df, list_of_columns_with_list_dtypes)
        df = flatten(df)
        list_of_columns_with_list_dtypes = has_list(df)
    return df

# COMMAND ----------

# Create the silver table by unpacking and enriching the dataframe
df = unpack(pandas_df)

spark_df = spark.createDataFrame(df)

spark_df = spark_df.withColumn('_source.duration', 
                               F.when(F.col('`_source.duration`').isin([float('inf'), float('-inf'), None]), 0)
                               .otherwise(F.col('`_source.duration`')))
spark_df = spark_df.withColumn('RoundedDuration', F.round(F.col('`_source.duration`'), 0).cast('int'))
spark_df = spark_df.withColumn('ExternalAssetEmbed', F.lit('filler'))
spark_df = spark_df.withColumn('IsHidden', F.lit('False'))
spark_df = spark_df.withColumn('DurationConverted', (F.from_unixtime('RoundedDuration', 'HH:mm:ss')))
spark_df = spark_df.withColumn('URLAnchorCombined', F.concat(F.col('_id'), F.col('`_source.mentions.anchor`')))
spark_df = spark_df.withColumn('CreatedOn', F.current_timestamp())

silver_df = spark_df

# COMMAND ----------

# Persist silver table to Delta lake
silver_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"/mnt/trainingsamp/training/{topic_name}/silver")

# COMMAND ----------

# Persist silver table to Delta lake - aggregations are to be done here when decided
gold_df = silver_df

gold_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"/mnt/trainingsamp/training/{topic_name}/gold")

