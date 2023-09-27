# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/saunextadls/raw/json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.withColumn("ingestion_date",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/saunextadls/raw/output/sids/json").saveAsTable("json.bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.bronze

# COMMAND ----------


