# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/bronze/customers")

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")
display(df)

# COMMAND ----------

df = df.withColumn("domains", split(col('email'), '@')[1])
display(df)

# COMMAND ----------

display(df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers", ascending=False))

# COMMAND ----------

df_gmail = df.filter(col('domains') == "gmail.com")
display(df_gmail)
time.sleep(5)

df_yahoo = df.filter(col('domains') == "yahoo.com")
display(df_yahoo)
time.sleep(5)

df_hotmail = df.filter(col('domains') == "hotmail.com")
display(df_hotmail)
time.sleep(5)

# COMMAND ----------

df = df.withColumn("full_name", concat(col('first_name'), lit(' '), col('last_name')))
df = df.drop('first_name', 'last_name')
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("/Volumes/workspace/default/silver/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;

# COMMAND ----------

# Volume se delta data read karein
df_cust = spark.read.format("delta").load("/Volumes/workspace/default/silver/customers")

# Usko workspace.silver.customers_silver table mein register karein
df_cust.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.customers_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.customers_silver LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.customers_silver;

# COMMAND ----------

