# Databricks notebook source
# MAGIC %pip install elasticsearch
# MAGIC %pip install azure-storage-blob
# MAGIC import pandas as pd
# MAGIC from elasticsearch import Elasticsearch, helpers
# MAGIC from azure.storage.blob import BlobServiceClient
# MAGIC import json
# MAGIC
# MAGIC # Flatten JSON 
# MAGIC def flatten(df):
# MAGIC     json_struct = json.loads(df.to_json(orient="records"))
# MAGIC     flattened_df = pd.json_normalize(json_struct)
# MAGIC     return flattened_df
# MAGIC
# MAGIC
# MAGIC # List checker
# MAGIC def has_list(x):
# MAGIC     def list_exists(x):
# MAGIC         return any(isinstance(i, list) for i in x)
# MAGIC     cols = df.apply(list_exists)
# MAGIC     cols_list = df.columns[cols].tolist()
# MAGIC     return cols_list
# MAGIC
# MAGIC
# MAGIC # Explode List object
# MAGIC def explode(df, cols):
# MAGIC     for col in cols:
# MAGIC         df = df.explode(col)
# MAGIC
# MAGIC     exploded_df = df
# MAGIC     return exploded_df
# MAGIC
