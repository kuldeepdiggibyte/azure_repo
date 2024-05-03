# Databricks notebook source
# MAGIC
# MAGIC %run /Workspace/Users/managoli.kuldeep2022@vitstudent.ac.in/silver_layer/customer

# COMMAND ----------

products = spark.read.csv('dbfs:/mnt/bronze/products',header=True,inferSchema=True)
display(products)

# COMMAND ----------


lower = snake_case(products)

display(lower)

# COMMAND ----------

sub_category_df = lower.withColumn("sub_category", when(col('category_id') == 1, "phone")\
        .when(col('category_id') == 2 , "laptop")\
        .when(col('category_id') == 3, "playstation")\
        .when(col('category_id') == 4, "e-device"))

display(sub_category_df)

# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/product'
write_delta_upsert(sub_category_df, writeTo)

# COMMAND ----------

