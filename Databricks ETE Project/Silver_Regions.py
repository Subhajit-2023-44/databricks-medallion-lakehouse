# Databricks notebook source
# 1. Ingest raw parquet data from volume and register as Bronze Delta table
df_temp = spark.read.format("parquet").load("/Volumes/workspace/default/source/regions")
df_temp.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.regions")

# 2. Read DataFrame from Bronze Delta table
df = spark.read.table("workspace.bronze.regions")
display(df)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/silver/regions")

# COMMAND ----------

df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/silver/products")

display(df)

# COMMAND ----------

# Ingest refined regions Delta data from volume
df_reg = spark.read.format("delta").load("/Volumes/workspace/default/silver/regions")

# Register regions_silver managed table in workspace.silver schema
df_reg.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.regions_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.regions_silver;
