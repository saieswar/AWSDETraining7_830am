from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import *
data=[(1,'2024-01-01',"I1",10,1000),(2,"2024-01-15","I2",20,2000),(3,"2024-02-01","I3",10,1500),(4,"2024-02-15","I4",20,2500),(5,"2024-03-01","I5",30,3000),(6,"2024-03-10","I6",40,3500),(7,"2024-03-20","I7",20,2500),(8,"2024-03-30","I8",10,1000)]
schema=["SOId","SODate","ItemId","ItemQty","ItemValue"]
spark = SparkSession.builder.appName("Student Grading").master("local").getOrCreate()
df1=spark.createDataFrame(data,schema)
df1.show()
df1.printSchema()

df1 = df1.withColumn("SODate", df1.SODate.cast(DateType()))
df1.show()
df1.printSchema()

df2 = df1.select(month(df1.SODate).alias("Month"), year(df1.SODate).alias("Year"), df1.ItemValue)
df2.show()

df3=df2.groupBy(df2.Month, df2.Year).agg(sum(df2.ItemValue).alias("TotalSale"))
df3.show()


df4 = df3.select("*", lag(df3.TotalSale).over(Window.orderBy(df3.Month, df3.Year)).alias("PrevSales") )
df4.show()


df4.printSchema()
df_final = df4.select('*',((df4.TotalSale-df4.PrevSales)*100/df4.TotalSale).alias("Percentage"))
df_final.show()
