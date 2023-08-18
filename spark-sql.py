from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col,pandas_udf
from pyspark.sql.types import LongType
import pandas as pd
import pyarrow
spark=SparkSession.builder.\
appName("sparkaql").\
getOrCreate()
# print(spark.version)
data=spark.read.format('csv').\
option('inferSchema','true').\
option('header','true').\
option('path','operations_management.csv').\
load()
# data.printSchema()
# process data
data_2=data.select("industry","value").filter((col("value")>200)& (col("industry")!="total")).\
orderBy(desc("value"))
data_2.printSchema()
data_2.show(5)
#create temprorary view to submit sql query on top of it
data_2.createOrReplaceTempView("data")
# spark.sql("""SELECT industry, value FROM data WHERE value>200 AND industry !="total"
        #   """).show(6)

#create view
data.createOrReplaceTempView("test")
data2=spark.sql("SELECT * FROM test")
data2.show()
# print(spark.catalog.listDatabases())
# print(spark.catalog.listTables())
#use pandas udf
def cubed(a: pd.Series)->pd.Series:
    return a*a*a
cubed_udf=pandas_udf(cubed,returnType=LongType())
x=pd.Series([1,2,3])
print(cubed(x))