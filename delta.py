# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_catalog.man_schema.deltatable
# MAGIC (
# MAGIC   id INT,
# MAGIC    name STRING,
# MAGIC    city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/deltalake/deltatable'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE man_catalog.man_schema.deltatable SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO man_catalog.man_schema.deltatable VALUES (1,'Sam','New York'),(2,'Amos','Los Angeles'),(3,'John','Chicago')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED man_catalog.man_schema.deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_catalog.man_schema.deltatable

# COMMAND ----------

# MAGIC %md
# MAGIC **UPDATES IN DELTA TABLE**

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE man_catalog.man_schema.deltatable SET city = 'Las Vagas' WHERE id = 3

# COMMAND ----------

# MAGIC %md
# MAGIC **VERSIONING**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY man_catalog.man_schema.deltatable

# COMMAND ----------

# MAGIC %md
# MAGIC **TIME TRAVEL**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE man_catalog.man_schema.deltatable TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC **DELETION VECTOR**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_catalog.man_schema.deltatable2
# MAGIC (
# MAGIC   id INT,
# MAGIC    name STRING,
# MAGIC    city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/deltalake/deltatable2'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO man_catalog.man_schema.deltatable2 VALUES (1,'Sam','New York'),(2,'Amos','Los Angeles'),(3,'John','Chicago')

# COMMAND ----------

# MAGIC %md
# MAGIC **UPDATES IN DELETION VECTOR TABLE**

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE man_catalog.man_schema.deltatable2 SET city = 'San Francisco' WHERE id = 3

# COMMAND ----------

# MAGIC %md
# MAGIC **OPTIMIZE DELTA TABLES**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history man_catalog.man_schema.deltatable2

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE man_catalog.man_schema.deltatable2 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deep Clone VS Shallow Clone

# COMMAND ----------

# MAGIC %md
# MAGIC **Deep clone**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_catalog.man_schema.deepclonetable  
# MAGIC DEEP CLONE man_catalog.man_schema.deltatable
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_catalog.man_schema.deepclonetable

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY man_catalog.man_schema.deepclonetable

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED man_catalog.man_schema.deepclonetable

# COMMAND ----------

# MAGIC %md
# MAGIC **Shsllow Clone**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_catalog.man_schema.shallowtable
# MAGIC SHALLOW CLONE man_catalog.man_schema.man_table
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED man_catalog.man_schema.shallowtable