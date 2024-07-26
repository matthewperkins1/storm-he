# Databricks notebook source
# Install libraries
# %pip install elasticsearch
# %pip install azure-storage-blob
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from azure.storage.blob import BlobServiceClient
import json
import pyspark.sql.functions as F

# COMMAND ----------

# Establish connection to Elasticsearch using credentials held in key vault backed secret scope.
key = dbutils.secrets.get('databricksscope', 'elastic-search-key')
cloud_id = dbutils.secrets.get('databricksscope', 'elastic-search-cloudid')
connection = Elasticsearch(cloud_id=cloud_id, api_key=key)
index = 'lhh-stories'

# COMMAND ----------

# Establish connection to ADLS for writing raw json to bronze layer
account_key = dbutils.secrets.get('databricksscope', 'mount-sas-account-token')

# Configure Blob Storage connection
blob_connection_string = f"DefaultEndpointsProtocol=https;AccountName=trainingsamp;AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_container_name = f"training/{index}/bronze"
blob_name = f"{index}.json"

# Create Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

# Create Blob Client
blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_name)

# COMMAND ----------

# Retrieve records from Elastic search and convert into Dataframe
records = list(helpers.scan(client=connection, index=index))


# COMMAND ----------

# Create json records and write bronze layer to ADLS
# Convert records to JSON string
json_records = json.dumps(records, indent=4)

# Upload JSON file to Blob Storage
blob_client.upload_blob(json_records, overwrite=True)

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

# Read in bronze json to spark DF for processing
spark_df = spark.read.option("multiline","true").json(f"/mnt/trainingsamp/training/{index}/bronze/{index}.json")

# COMMAND ----------

# Create the silver layer by unpacking and enriching the dataframe
pdf = spark_df.toPandas()

# Create the silver table by unpacking and enriching the dataframe
df = unpack(pdf)

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
    .save(f"/mnt/trainingsamp/training/{index}/silver")

# COMMAND ----------

# Persist silver table to Delta lake - aggregations are to be done here when decided
gold_df = silver_df

gold_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"/mnt/trainingsamp/training/{index}/gold")

