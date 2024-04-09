from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("first") \
    .master("local")\
    .getOrCreate()

# Create DataFrame df1
data1 = [("1",), ("2",), ("3",)]
df1 = spark.createDataFrame(data1, ["col"])

df1.show()

# Create DataFrame df2
data2 = [("1",), ("2",), ("3",), ("4",), ("5",)]
df2 = spark.createDataFrame(data2, ["col"])

df2.show()

maxdf = df1.selectExpr("max(col) as col ")
maxdf.show()

lantijoin = df2.join(maxdf, df2.col == maxdf.col, "left_anti")

lantijoin.show()