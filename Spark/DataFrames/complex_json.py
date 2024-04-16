#single Line Json
#multi Line Json
#single line nested json
#Multi line nested json
#Json is semi structed format


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ComplexJson").getOrCreate();
df = spark.read.format('json').option("multiline", True).load("./inputData/comp.json")
df.show(truncate=False)
df.printSchema()
#df.select(explode("batters.batter")).show()
df_final = df.withColumn("topping_explode", explode("topping"))\
             .withColumn("topping_id", col("topping_explode.id"))\
             .withColumn("topping_type", col("topping_explode.type"))\
             .withColumn("batter_explode", explode("batters.batter"))\
             .withColumn("batter_id", col("batter_explode.id"))\
             .withColumn("batter_type", col("batter_explode.type"))\
             .drop("batters","topping", "batter_explode", "topping_explode")

df_final.show()
#complex - struct, array, map

# df1 = spark.read.format("json").option("multiline", True).load("./inputData/dynamic_complex.json")
# df1.show()
# df1.printSchema()
#
#
# def master_array(df):
#     array_cols = s