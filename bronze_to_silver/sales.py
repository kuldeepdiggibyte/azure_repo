# Databricks notebook source
# MAGIC %run /Workspace/Users/managoli.kuldeep2022@vitstudent.ac.in/silver_layer/customer

# COMMAND ----------

sales_df =  spark.read.csv('dbfs:/mnt/bronze/sales/20240107_sales_data.csv', header=True, inferSchema=True)


# COMMAND ----------

sales_renamed= snake_case(sales_df)

# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/customer_sales'
write_delta_upsert(sales_renamed, writeTo)

# COMMAND ----------

