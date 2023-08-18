from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import RFormula
import pandas as pd
import click
#create session
spark = SparkSession.builder.appName("App").getOrCreate()
airbnbDF=spark.read.parquet('C:\Spark-tutorial\ML_Data.parquet')
airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
"number_of_reviews", "price").show(5)
trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)

print(f"""There are {trainDF.count()} rows in the training set,
and {testDF.count()} in the test set""")
vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")

vecTrainDF = vecAssembler.transform(trainDF)

vecTrainDF.select("bedrooms", "features", "price").show(10)
#linear regression to model linear relation to predict price of rental price  
lr = LinearRegression(featuresCol="features", labelCol="price")

lrModel = lr.fit(vecTrainDF)
m = round(lrModel.coefficients[0], 2)
b = round(lrModel.intercept, 2)

print(f"""The formula for the linear regression line is
price = {m}*bedrooms + {b}""")
#linear regression to model linear relation to predict price of rental price  
pipeline = Pipeline(stages=[vecAssembler, lr])

pipelineModel = pipeline.fit(trainDF)
predDF = pipelineModel.transform(testDF)

predDF.select("bedrooms", "features", "price", "prediction").show(10)
categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]

indexOutputCols = [x + "Index" for x in categoricalCols]

oheOutputCols = [x + "OHE" for x in categoricalCols]

stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")

oheEncoder = OneHotEncoder(inputCols=indexOutputCols, outputCols=oheOutputCols)

numericCols = [field for (field, dataType) in trainDF.dtypes if ((dataType == "double") & (field != "price"))]

assemblerInputs = oheOutputCols + numericCols

vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
rFormula = RFormula(formula="price ~ .", featuresCol="features", labelCol="price", handleInvalid="skip")
lr = LinearRegression(labelCol="price", featuresCol="features")

pipeline = Pipeline(stages = [stringIndexer, oheEncoder, vecAssembler, lr])

# Or use RFormula
# pipeline = Pipeline(stages = [rFormula, lr])

pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)

predDF.select("features", "price", "prediction").show(5)
regressionEvaluator = RegressionEvaluator( 
predictionCol="prediction",
labelCol="price",
metricName="rmse")

rmse = regressionEvaluator.evaluate(predDF)

print(f"RMSE is {rmse:.1f}")
r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)

print(f"R2 is {r2}")
