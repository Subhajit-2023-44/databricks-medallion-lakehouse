# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/bronze/orders")

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed('_rescued_data', 'rescued_data')
display(df)

# COMMAND ----------

df = df.drop("rescued_data")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

df = df.withColumn("order_date", to_timestamp(col('order_date')))
display(df)

# COMMAND ----------

from pyspark.sql.functions import year

df = df.withColumn("year", year(col('order_date')))
display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

df1 = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import rank

df1 = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import row_number

df1 = df1.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Classes - OOP**

# COMMAND ----------

class windows:
    
    def dense_rank(self, df):
        df_dense_rank = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_dense_rank

    def rank(self, df):
        df_rank = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank

    def row_number(self, df):
        df_row_number = df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/silver/orders")

# COMMAND ----------

# Volume se orders delta data read karein
df_orders = spark.read.format("delta").load("/Volumes/workspace/default/silver/orders")

# workspace.silver schema mein orders_silver table register karein
df_orders.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.orders_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.orders_silver LIMIT 5;