# Databricks notebook source
# MAGIC %md
# MAGIC ### Concept - 1
# MAGIC **Managed Catalog**
# MAGIC **- Managed Schema**
# MAGIC **- Managed Table**

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Catalog**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA man_catalogl;

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Schema/Database**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA man_catalog.man_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_catalog.man_schema.man_table
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT,
# MAGIC     gender STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concept - 2
# MAGIC **External Catalog**
# MAGIC **- Managed Schema**
# MAGIC **- Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG ext_cata
# MAGIC MANAGED location 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/external_catalog';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ext_cata.man_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_cata.man_schema.man_table2
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT,
# MAGIC     gender STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/external_catalog/man_schema/man_table2';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concept - 3
# MAGIC **External Catalog**
# MAGIC **- External Schema**
# MAGIC **- Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ext_cata.ext_schema
# MAGIC MANAGED LOCATION 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/ext_schema'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_cata.ext_schema.man_table3
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT,
# MAGIC     gender STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concept - 4
# MAGIC **- External Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_catalog.man_schema.ext_table
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT,
# MAGIC     gender STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/ext_table/ext_table';
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO man_catalog.man_schema.ext_table
# MAGIC VALUES
# MAGIC   (1, 'John', 30, 'Male'),
# MAGIC   (2, 'Jane', 25, 'Female'),
# MAGIC   (3, 'Bob', 40, 'Male'),
# MAGIC   (4, 'Alice', 35, 'Female'),
# MAGIC   (5, 'Charlie', 28, 'Male'),
# MAGIC   (6, 'Diana', 32, 'Female');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_catalog.man_schema.ext_table;

# COMMAND ----------

# MAGIC %md
# MAGIC **Query Files Using SELECT**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/ext_table/ext_table` WHERE age > 30

# COMMAND ----------

# MAGIC %md
# MAGIC **DROP Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE man_catalog.man_schema.man_table;

# COMMAND ----------

# MAGIC %md
# MAGIC **RECOVER MANAGED TABLE(UNDROP)**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE man_catalog.man_schema.man_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE PERMANENT VIEWS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW man_catalog.man_schema.view1 
# MAGIC AS 
# MAGIC SELECT * FROM DELTA.`abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/ext_table/ext_table` WHERE age > 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_catalog.man_schema.view1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TEMP VIEWS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view
# MAGIC AS 
# MAGIC SELECT * FROM DELTA.`abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/ext_table/ext_table` WHERE age > 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC ### VOLUMES
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a Directory for Volume

# COMMAND ----------


dbutils.fs.mkdirs('abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/volumes')

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a Volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME man_catalog.man_schema.ext_volume
# MAGIC LOCATION 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/volumes';

# COMMAND ----------

# MAGIC %md
# MAGIC **Copy file for Volume**

# COMMAND ----------

dbutils.fs.cp('abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/source/Revenue', 'abfss://mycontainer@databricksstoragesam.dfs.core.windows.net/volumes/Revenue', recurse=True)


# COMMAND ----------

# MAGIC %md
# MAGIC **Query Volumes**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/Volumes/man_catalog/man_schema/ext_volume/Revenue`

# COMMAND ----------

