# Databricks notebook source
# MAGIC %pip install elasticsearch
# MAGIC %pip install azure-storage-blob
# MAGIC import pandas as pd
# MAGIC from elasticsearch import Elasticsearch, helpers
# MAGIC from azure.storage.blob import BlobServiceClient
# MAGIC import json
