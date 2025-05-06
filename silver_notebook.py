# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data
# MAGIC

# COMMAND ----------

df = spark.read.format("csv")\
                    .option("header", "true")\
                        .option("inferSchema", "true")\
                        .load("abfss://silver@bankingdatalakesam.dfs.core.windows.net/transformeddata")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.groupBy("customer_name","city","products","amount","payment_mode").count().orderBy("count", ascending=False).display()

# COMMAND ----------

df_refined = df.selectExpr("customer_name","city","products","amount","profit","payment_mode")

# COMMAND ----------

display(df_refined)

# COMMAND ----------

from pyspark.sql.functions import sum

df_final = df.groupBy("city","customer_name","payment_mode").agg(sum("amount").alias("total_amount"), sum("profit").alias("total_profit")).distinct().limit(20)

# COMMAND ----------

df_final.display()


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writting Data To Gold Layer

# COMMAND ----------

df_final.write.format("delta")\
                .mode("overwrite")\
                    .save("abfss://gold@bankingdatalakesam.dfs.core.windows.net/curatedData")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning Data

# COMMAND ----------

df_final.write.mode("append").partitionBy("city").format("delta").save("abfss://gold@bankingdatalakesam.dfs.core.windows.net/partitioned")

# COMMAND ----------

