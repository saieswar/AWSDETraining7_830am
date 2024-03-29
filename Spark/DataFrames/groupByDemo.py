from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,max,count,sum,min

spark = SparkSession.builder.appName("Group By Demo").master("local").getOrCreate()

df = spark.read.format("csv").options(header=True, inferSchema=True).load("./inputData/StudentData.csv")

df.printSchema()

#df.groupBy("name").agg({"age": "sum"}).sort("name").show()

df.groupBy("gender").sum("marks").show()
#df.groupBy("gender").agg("marks":)

print("================= Aggeregations Group by ==============")

df.groupBy("gender")\
    .agg(sum("marks").alias("sum_salary"),\
         avg("marks").alias("avg_marks"),\
         max("marks").alias("max_marks"),\
         min("marks").alias("min_marks")
         )\
    .show()





df.groupBy("gender", "age")\
    .agg(sum("marks").alias("sum_salary"),\
         avg("marks").alias("avg_marks"),\
         max("marks").alias("max_marks"),\
         min("marks").alias("min_marks")
         )\
    .show()

# gender wise sum the marks
# gender, tot_marks
# M, 600
# F , 100


#create student_data_analytics.py

#total number of students enrollerd in each course
#total number of male and female students in each course
#total marks achieved by each gender in each course
#min, max, avg marks achieved in each course by each age group