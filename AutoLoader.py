# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC **AUTO LOADER**

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming DataFrame**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/autodestination/check")\
    .load("abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/autosource")


# COMMAND ----------

df.writeStream.format("parquet")\
    .option("checkpointLocation", "abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/autodestination/check")\
    .trigger(processingTime="10 seconds")\
    .start("abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/autodestination/data")


# COMMAND ----------

