# Databricks notebook source
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
