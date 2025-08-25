from pyspark.sql import SparkSession
import pandas as pd
import time
from pyspark.sql.types import IntegerType, Row

def time_convert(sec):
  mins = sec // 60
  sec = sec % 60
  hours = mins // 60
  mins = mins % 60
  print("Time Lapsed = {0}:{1}:{2}".format(int(hours),int(mins),sec))

# spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").enableHiveSupport().getOrCreate()
# # spark.sql("show databases;").show()
# result = spark.sql("select * from test.pokes")
# result.collect()
# print(result)
# spark = SparkSession.builder.appName("").config('spark.sql.catalogImplementation','in-memory').enableHiveSupport().getOrCreate()
# spark = SparkSession.builder.appName("").config('spark.sql.hive.metastore.version','2.1.1').config('spark.sql.hive.metastore.jars','/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hive/lib').enableHiveSupport().getOrCreate()
# spark = SparkSession.builder.appName("").config('spark.sql.hive.metastore.version','2.1.1').config('spark.sql.hive.metastore.jars','/opt/cloudera/parcels/CDH/lib/hive/lib').enableHiveSupport().getOrCreate()
# spark = SparkSession.builder.appName("").config('spark.sql.hive.metastore.version','2.1.1').config('spark.sql.hive.metastore.jars','/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars').enableHiveSupport().getOrCreate()
# spark = SparkSession.builder.appName("").config('spark.sql.hive.metastore.version','2.1.1').config('spark.sql.hive.metastore.jars','maven').enableHiveSupport().getOrCreate()

# spark.sparkContext.setCheckpointDir("/home/absuser/spark/checkpoint")
#spark.sparkContext.setCheckpointDir("/user/spark/checkpoint")

# Standlone
# spark = SparkSession.builder.appName("").enableHiveSupport().getOrCreate()
# spark.sparkContext.setLogLevel("WARN")
# ---------------------------------------------------------------------------
# On Yarn
spark = SparkSession.builder.config("spark.executor.memory","13g").config("spark.driver.memory","13g").master("yarn").enableHiveSupport().getOrCreate()
spark.sparkContext.setCheckpointDir("/user/spark/checkpoint")
spark.sparkContext.setLogLevel("WARN")

#spark.conf.set("spark.executor.memory","10g")
#spark.sql("set spark.sql.shuffle.partitions=1")




# spark.sql("set spark.sql.shuffle.partitions=1")

#Spark 3.1.3
# df = spark.sql("select * from test.pokes") 
#Spark 2.4.4
# df = spark.sql("select * from testdata.pokes")
# spark.sql("use test")
# df = spark.sql("show databases")
# df = spark.sql("show tables")
# df.collect()
# dfp = df.toPandas()
# print(dfp)
start_time = time.time()

df = spark.sql('select p.trustcode,p.accountno,p.payamount+i.payamount from testdata.principalschedule p join testdata.interestschedule i on p.trustcode=i.trustcode and p.accountno=i.accountno and p.paydate=i.paydate')
#df1=df.repartition(1).checkpoint()


print(df.count())
end_time = time.time()
time_lapsed = end_time - start_time
print(time_convert(time_lapsed))
# dfp = df.toPandas()
# print(dfp)

