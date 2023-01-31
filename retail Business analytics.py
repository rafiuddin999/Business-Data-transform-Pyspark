# Databricks notebook source
dbutils.fs.mount(source = "wasbs://rafidata@adlsstrgdb.blob.core.windows.net",mount_point = "/mnt/silv",
extra_configs ={"fs.azure.account.key.adlsstrgdb.blob.core.windows.net":'hkxruKf48RSxljlC3ILBk7KAfSsGTswA66gTVNZu8js2jta3Gbk7Dfoyd6s/hX3e2yEx6AWjPuyN+ASt4ckOWQ=='})

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([\
                    StructField("id", StringType(),True),\
                    StructField("fname", StringType(),True),\
                    StructField("lname", StringType(),True),\
                    StructField("mobile", StringType(),True),\
                    StructField("home", StringType(),True),\
                    StructField("Address", StringType(),True),\
                    StructField("city", StringType(),True),\
                    StructField("state", StringType(),True),\
                    StructField("Zip_code", StringType(),True)])

# COMMAND ----------

scenario_1 = spark.read.csv('dbfs:/mnt/silv/data-files/data-files/customers-tab-delimited/part-m-00000',sep="\t",schema=schema)
display(scenario_1)

# COMMAND ----------

from pyspark.sql.functions import *

#from pyspark.sql.functions import concat

# show the client info who is in california
scenario_01 = scenario_1.filter(scenario_1.state == 'CA').withColumn('full_name',concat(scenario_1.lname,lit(' '),scenario_1.lname))
scenario_001 = scenario_01.select('id','full_name','Address','city','state','Zip_code')
#final ouput in text format
result = scenario_001.withColumn('output',concat(scenario_001.id,lit(' '),\
                             scenario_001.full_name,lit(' '),\
                             scenario_001.Address,lit(' '),\
                             scenario_001.city,lit(' '),\
                             scenario_001.state,lit(' '),\
                             scenario_001.Zip_code,lit(' '))).select('output')
result.write.option('sep','\t').mode('overwrite').text("/mnt/silv/result/scenario1/solution/")
display(result)
#save in result/scenario1/solution 
#only with code CA
#full name in a column
# scenario_001.coalesce(1).write.format("text").option("header", "false").mode("append").save("/mnt/silv/result/scenario1/solution/output.txt")


# COMMAND ----------

# MAGIC %md ##TASK -->> 2.3

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/silv/data-files/data-files/orders_parquet/

# COMMAND ----------

order = spark.read.parquet('dbfs:/mnt/silv/data-files/data-files/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet')
display(order)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import *

# from pyspark.sql.types import StringType
orders = order.filter(order.order_status == 'COMPLETE').withColumn('order_date_time',from_utc_timestamp(from_unixtime(order.order_date/1000,"yyyy-MM-dd hh:mm:ss"),'GMT+5')).select('order_id','order_date_time','order_status')
display(orders)

# COMMAND ----------

orders.write.option("compression","gzip").json("/mnt/silv/result/scenario2/solution/")


# COMMAND ----------

# MAGIC %md ###TASK 3

# COMMAND ----------

scenario_3 = spark.read.csv('dbfs:/mnt/silv/data-files/data-files/customers-tab-delimited/part-m-00000',sep="\t",schema=schema)
display(scenario_3)

# COMMAND ----------

scenario_03 = scenario_3.filter(scenario_3.city == 'Caguas')

# COMMAND ----------

scenario_03.coalesce(1).write.format('orc').option("compression","snappy").save("/mnt/silv/result/scenario3/solution/output.orc")

# COMMAND ----------

# MAGIC %md ###TASK 4

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sqema = StructType([\
                    StructField("id", IntegerType(),False),\
                    StructField("matches_won", IntegerType(),True),\
                    StructField("sports", StringType(),True)\
                    ])
scenario_4 = spark.read.csv('dbfs:/mnt/silv/data-files/data-files/categories/part-m-00000',schema=sqema)

scenario_4.write.mode('overwrite').option("compression","lz4").csv("/mnt/silv/result/scenario4/solution/")

# COMMAND ----------

# MAGIC %md ##TASK 5

# COMMAND ----------

scenario_5 = spark.read.format("avro").load('dbfs:/mnt/silv/data-files/data-files/products_avro/*')
scenario_5.show()

# COMMAND ----------

scenario_05 = scenario_5.filter(scenario_5.product_price >= 1000.0)
scenario_05.write.mode('overwrite').option("compression","snappy").parquet("/mnt/silv/result/scenario5/solution/")

# COMMAND ----------

# MAGIC %md ##TASK 6

# COMMAND ----------

scenario_6 = spark.read.format('avro').load('dbfs:/mnt/silv/data-files/data-files/products_avro/*')
scenario_06 =  scenario_6.filter(scenario_6.product_price >= 1000.0).filter(scenario_6.product_name.contains('Treadmill'))
scenario_06.show(truncate=False)
scenario_06.write.mode('overwrite').option("compression","gzip").parquet("/mnt/silv/result/scenario6/solution/")

# COMMAND ----------

# MAGIC %md ##TASK 7

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,from_unixtime

scenario_7 = spark.read.parquet('dbfs:/mnt/silv/data-files/data-files/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet')
# date in yyy mm  dd 
scenario_07 = scenario_7.withColumn('order_date',from_utc_timestamp(from_unixtime(scenario_7.order_date/1000,"yyyy-MM-dd"),'GMT+5')) 
# output all pending orders in jul 2013
scenario_007 = scenario_07.filter((scenario_07.order_date > '2013-07-01 00:00:00') &(scenario_07.order_date < '2013-07-31 00:00:00')).filter(scenario_07.order_status.contains('PENDING_PAYMENT')).select('order_date','order_status')
# json format snappy with order date and status
scenario_007.write.mode('overwrite').option("compression","snappy").json("/mnt/silv/result/scenario7/solution/")

# COMMAND ----------


