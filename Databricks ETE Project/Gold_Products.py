# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Table**

# COMMAND ----------

# Expectations
my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# COMMAND ----------

@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = (spark.readStream
          .option("skipChangeCommits", "true")
          .table("workspace.silver.products_silver"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming View**

# COMMAND ----------

@dlt.view
def DimProducts_view():
    df = spark.readStream.table("LIVE.DimProducts_stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **DimProducts**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
    target = "DimProducts",
    source = "Live.DimProducts_view",
    keys = ["product_id"],
    sequence_by = "product_id",
    stored_as_scd_type = 2
)