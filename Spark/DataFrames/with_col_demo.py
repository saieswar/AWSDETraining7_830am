from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.appName("withColDemo").master("local").getOrCreate()

df = spark\
    .read\
    .format("csv")\
    .options(header=True, inferSchema=True)\
    .load("./inputData/StudentData.csv")

df.show()

df1 = df.withColumn("total_marks", lit(100))
df1.show()
df2 = df1.withColumn("percentage", (df1.marks/df1.total_marks)*100)
df2.show()

print("=================   Example ======================")
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

emp_df  = spark.createDataFrame(data=data, schema = columns)

#emp1 =emp_df.withColumn("sal_100", emp_df.salary *100)
#emp_df.withColumn("salary", col("Salary").cast("Integer"))
emp1 =emp_df.withColumn("sal_100", col("salary") +100)
emp1.show()









