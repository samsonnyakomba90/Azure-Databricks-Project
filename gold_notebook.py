# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data 

# COMMAND ----------

df = spark.read.format("delta")\
                    .option("header", "true")\
                        .option("inferSchema", "true")\
                            .load("abfss://gold@bankingdatalakesam.dfs.core.windows.net/partitioned")
                          

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **filtering Data**

# COMMAND ----------

df_cred_deb = df.filter("payment_mode == 'Credit'or payment_mode == 'Debit'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **visualize**

# COMMAND ----------

df_city = df.filter("city == 'Cleveland'or city == 'Chicago' or city == 'San Francisco'").display()

# COMMAND ----------

df_max_profit = df.filter("total_profit > 1000").display()

# COMMAND ----------

