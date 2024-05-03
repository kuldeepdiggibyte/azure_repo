# Databricks notebook source
# MAGIC %run /Workspace/Users/managoli.kuldeep2022@vitstudent.ac.in/silver_layer/customer

# COMMAND ----------

store = spark.read.csv('dbfs:/mnt/bronze/store',header=True,inferSchema=True)
display(store)

# COMMAND ----------

store = snake_case(store)

# COMMAND ----------

def domain(df):
    df = df.withColumn('domain',split(df['email_address'],'@').getItem(1))
    df2 = df.withColumn('domain',split(df['domain'],'\.').getItem(0))
    return df2

display(domain(store))

# COMMAND ----------

from pyspark.sql.functions import *
store = store.withColumn('created_at', to_date('created_at', 'dd-MM-yyyy'))\
    .withColumn('updated_at', to_date('updated_at', 'dd-MM-yyyy'))

# COMMAND ----------

display(store)

# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/store'
write_delta_upsert(store, writeTo)

# COMMAND ----------



# COMMAND ----------

