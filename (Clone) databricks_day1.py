# Databricks notebook source
print('Hello Python')

# COMMAND ----------

# MAGIC %sql
# MAGIC select "hello SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC #this is sample text

# COMMAND ----------

# MAGIC %sql
# MAGIC create database test;
# MAGIC use test;
# MAGIC create table test.demo(id int, name string);

# COMMAND ----------



users=[(1,'a',30,"Sales"),(2,'b',25,"IT"),(3,'c',28,"Data Science")]

schema="id int, name string, age int, dept string"

df=spark.createDataFrame(data=users, schema=schema)

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1= df.withColumnRenamed("id","praket").withColumn('Current date',current_date)
df1.show()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('emp_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use test;
# MAGIC
# MAGIC select * from emp_table

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df = spark.read.option('header',True).option('inferschema',True).csv('dbfs:/FileStore/tables/addresses.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('test_add')

# COMMAND ----------

dbutils.fs.mount(

  source = "wasbs://raw@sanly.blob.core.windows.net",

  mount_point = "/mnt/sanly/raw",

  extra_configs = {"fs.azure.account.key.sanly.blob.core.windows.net":"+wZyMJdwqiETIzCNMc/uvE0AJQ/2+fIGVKKvfx4um7lsUO0EPZjLx3efLhF9OihDdkaV1TBwq77j+AStSZRQ1Q=="})

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw

# COMMAND ----------

df = spark.read.option('header',True).option('inferschema',True).csv('dbfs:/mnt/sanly/raw/Baby_Names.csv')

# COMMAND ----------

df.show()

# COMMAND ----------


output="dbfs:/mnt/sanly/raw/output"
df.write.mode("overwrite").parquet(f"{output}/praket/babyname")

# COMMAND ----------

dbutils.fs.unmount("/mnt/sanly/raw")

# COMMAND ----------

dbutils.fs.mount(

  source = "wasbs://inputfiles@saunext.blob.core.windows.net",

  mount_point = "/mnt/saunext/inputfiles",

  extra_configs = {"fs.azure.account.key.saunext.blob.core.windows.net":"UUDMjjk8JYIiTwHNyh8WCs3BShkfIL//HM/cUrbOrRmUH+HaoR/J5bM9MlWTYefbkqNo/bQzgs1M+AStEn3dkA=="})

# COMMAND ----------

users_sch="timestamp timestamp, event_type string, user_id string, page_id string"
df=spark.readStream.schema(users_sch).json("dbfs:/mnt/saunext/inputfiles/inputstream/")
df.display()

# COMMAND ----------

outputstream="dbfs:/mnt/saunext/inputfiles/outputstream"
df.writeStream\
.option("checkpointlocation",f"{outputstream}/naval/checkpoint")\
.option("path",f"{outputstream}/Praket/output")\
.table("test.jsonsample")

# COMMAND ----------

for stream in spark.streams.active:

    stream.stop()

# COMMAND ----------

(spark

.readStream

.schema(users_sch)

.json("dbfs:/mnt/saunext/inputfiles/inputstream/")

.writeStream

.option("checkpointlocation",f"{outputstream}/naval/checkpoint")

.option("path",f"{outputstream}/naval/output")

.trigger(once=True)

.table("test.jsonsample")

)
