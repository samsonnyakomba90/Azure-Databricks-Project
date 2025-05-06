# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------


df = spark.read.format("csv")\
                    .option("header", True)\
                        .option("inferSchema", True)\
                            .load("abfss://bronze@bankingdatalakesam.dfs.core.windows.net/rawdata/sales.csv")
display(df)
                

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Library**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC **renaming columns**

# COMMAND ----------

 df1 = df.withColumnRenamed("Category", "category")\
        .withColumnRenamed("Sub-Category", "sub-category")\
        .withColumnRenamed("Order ID", "order_id")\
        .withColumnRenamed("Amount", "amount")\
        .withColumnRenamed("Profit", "profit")\
        .withColumnRenamed("Quantity", "quantity")\
        .withColumnRenamed("PaymentMode", "payment_mode")\
        .withColumnRenamed("Order Date", "order_date")\
        .withColumnRenamed("CustomerName", "customer_name")\
        .withColumnRenamed("State", "state")\
        .withColumnRenamed("City", "city")\
        .withColumnRenamed("Year-Month", "date")


display(df1)

# COMMAND ----------

df1 = df1.withColumnRenamed("sub_category", "products")\
            .withColumnRenamed("order_date", "supplier_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **fixing data types .cast()**

# COMMAND ----------

df1 = df1.withColumn("amount", col("amount").cast("double"))\
        .withColumn("profit", col("profit").cast("double"))\

        




# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Selecting Columns**

# COMMAND ----------

df_transformed = df1.selectExpr("order_id", "customer_name","products","quantity", "amount", "profit", "payment_mode", "supplier_date","date", "city")

# COMMAND ----------

display(df_transformed)


# COMMAND ----------

df_transformed = df_transformed.withColumn("payment_mode", split(col("payment_mode"), " ").getItem(0))

# COMMAND ----------

display(df_transformed)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Writting Data To Silver Layer

# COMMAND ----------

df_transformed.write.format("csv")\
                    .mode("append")\
                    .option("header", True)\
                    .option("inferSchema", True)\
                    .save("abfss://silver@bankingdatalakesam.dfs.core.windows.net/transformeddata")

# COMMAND ----------

