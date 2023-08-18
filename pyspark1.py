from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName ("test")\
    .getOrCreate()
data=[
    ('james','','1991-04-01','M',3000),
    ('Robert','','1991-03-01','M',3000),
    ('Maria','','1991-05-01','M',4000),
    ('jen','','1991-02-01','M',5000),
]
columns=["firstname","middlename","dob","gender","salary"]
df=spark.createDataFrame(data=data,schema=columns)
print(df.printSchema())