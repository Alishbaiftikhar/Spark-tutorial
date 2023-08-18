from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import * 
from pyspark.sql.types import *
spark = SparkSession.builder \
    .master("local[*]") \
    .appName ("test")\
    .getOrCreate()
# import pdb;pdb.set_trace()
df = spark.read.csv('datacamp_ecommerce.csv',header=True,escape="\"")
df.show(5,0)
print(df.count()) 
print(df.select('CustomerID').distinct().count())
# print(df.groupBy('Country').agg(count_distinct('CustomerID').alias('country_count')).show())
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn('date',to_timestamp("InvoiceDate", 'yy/MM/dd HH:mm'))
print(df.select(max("date")).show())
