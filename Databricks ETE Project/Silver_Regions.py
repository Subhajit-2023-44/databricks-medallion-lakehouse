# Databricks notebook source
# 1. Volume se parquet data read karke bronze schema mein Delta table register karein
df_temp = spark.read.format("parquet").load("/Volumes/workspace/default/source/regions")
df_temp.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.regions")

# 2. Table se DataFrame read karein (bilkul Ansh ki tarah)
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

# Volume se regions delta data read karein
df_reg = spark.read.format("delta").load("/Volumes/workspace/default/silver/regions")

# workspace.silver schema mein regions_silver table register karein
df_reg.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.regions_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.regions_silver;