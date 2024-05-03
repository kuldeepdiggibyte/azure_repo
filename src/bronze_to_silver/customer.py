# Databricks notebook source


# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

customer = spark.read.csv('dbfs:/mnt/bronze/customer/20240106_sales_custmer.csv',header=True,inferSchema=True)
display(customer)

# COMMAND ----------

def snake_case(df):
    for i in df.columns:
        df = df.withColumnRenamed(i,i.lower().replace(" ","_"))
    return df


# COMMAND ----------

customerdf = snake_case(customer)
display(customerdf)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import split
def split_function(df):
    df1 = customerdf.withColumn('first_name',split(customerdf["name"]," ")[0])
    df1 = df1.withColumn('last_name',split(customerdf["name"]," ")[1])
    df1 = df1.drop("name")
    return df1


# COMMAND ----------

display(split_function(customerdf))

# COMMAND ----------

def domain(df):
    df = df.withColumn('domain',split(df['email_id'],'@').getItem(1))
    df2 = df.withColumn('domain',split(df['domain'],'\.').getItem(0))
    return df2

display(domain(customerdf))

# COMMAND ----------

from pyspark.sql.functions import when
def gender_col(df):
    df = df.withColumn('gender',when(df["gender"] == 'male' , 'M').when(df['gender']=='female','F'))
    return df

display(gender_col(customerdf))

# COMMAND ----------

def split_date_time(df):
    df = df.withColumn('date',split(df['joining_date'],' ').getItem(0))
    df=df.withColumn('time',split(df['joining_date'],' ').getItem(1))
    return df

# COMMAND ----------

customerdf = split_date_time(customerdf)
display(customerdf)

# COMMAND ----------

from pyspark.sql.functions import *
def date(df):
    df = df.withColumn('date',to_date('date','dd-MM-yyyy'))
    return df 

customerdf = date(customerdf)
display(customerdf)

# COMMAND ----------


def expenditure_status(df):
    df = df.withColumn('expenditure-status',when(df['spent'] < 200, "Minimum").when(df['spent']>200,'Maximum'))
    return df

customerdf = expenditure_status(customerdf)

# COMMAND ----------

display(customerdf)

# COMMAND ----------

# dbutils.fs.mount(
#     source = 'wasbs://silver@bhaustorage.blob.core.windows.net/',
#     mount_point = '/mnt/silver',
#     extra_configs = {'fs.azure.account.key.bhaustorage.blob.core.windows.net':'EOJJY4z6iC9RH2CuaIfbmVyBXGkE8yHxGCMWqCnHZIXsvWcNQXzHxIrVLK1YBmqwXHtmf1XQACCP+ASth50Mgw=='}

# )

# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)

# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/customer'
customerdf.write.format('delta').mode('overwrite').save(writeTo)

# COMMAND ----------

#%pip install delta-spark

# COMMAND ----------

