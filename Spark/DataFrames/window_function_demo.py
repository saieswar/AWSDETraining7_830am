from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,dense_rank,rank
spark = SparkSession.builder.appName("WindowDemo").master("local").getOrCreate()

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema=columns)
df.show()

windowSpec = Window.partitionBy("department").orderBy("salary")
print("=========== Row Number =====================")
df.withColumn("row_number", row_number().over(windowSpec))\
  .withColumn("ranks", rank().over(windowSpec))\
  .withColumn("dense_rank", dense_rank().over(windowSpec))\
  .show()

