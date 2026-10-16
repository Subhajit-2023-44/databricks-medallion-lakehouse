# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dynamic Capabilities**

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

# Batch read to preview source data
df_preview = spark.read.format("parquet").load("/Volumes/workspace/default/source/orders")
display(df_preview.limit(10))

# COMMAND ----------

# Dynamic Auto Loader reading configuration
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/default/bronze/checkpoint_{p_file_name}")
    .load(f"/Volumes/workspace/default/source/{p_file_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

# Dynamic Auto Loader writing configuration
df.writeStream.format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", f"/Volumes/workspace/default/bronze/checkpoint_{p_file_name}") \
    .option("path", f"/Volumes/workspace/default/bronze/{p_file_name}") \
    .trigger(once=True) \
    .start()

# COMMAND ----------

df = spark.read.format("parquet").load("/Volumes/workspace/default/bronze/orders")
df.display()

# COMMAND ----------

df = spark.read.format("parquet") \
    .load(f"/Volumes/workspace/default/bronze/{p_file_name}")

display(df)