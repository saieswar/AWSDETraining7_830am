from pyspark.sql import  SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("NewApp").getOrCreate()

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df = spark.createDataFrame(data= data, schema=schema)

df.show()
df.printSchema()

print("=========== Create DF Using File read ======================")

df1 = spark.read.format('csv').options(header=True).load('./inputData/StudentData.csv')
df1.show()

print("========= select using names ==========")
df2 = df1.select("name", "age")
df2.show()

print("======== Select using Col function ================")
df1.select(col('name'), col('marks')).show()

print("===================== Select using . ===============")
df1.select(df1.name, df1.age).show()

