# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/bronze/products")

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")
display(df)

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, workspace.bronze.discount_func(price) as discounted_price
# MAGIC FROM products

# COMMAND ----------

df = df.withColumn("discounted_price", expr("workspace.bronze.discount_func(price)"))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC $$
# MAGIC   return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, brand, workspace.bronze.upper_func(brand) as brand_upper
# MAGIC FROM products

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "/Volumes/workspace/default/silver/products") \
    .save()

# COMMAND ----------

# Read refined products Delta data from volume and register managed table
df_prod = spark.read.format("delta").load("/Volumes/workspace/default/silver/products")
df_prod.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.products_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.products_silver LIMIT 5;
