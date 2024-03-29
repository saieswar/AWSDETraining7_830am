from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Filter Demo").master("local").getOrCreate()

df = spark.read.format('csv').options(header=True).load("./inputData/StudentData.csv")
#df = spark.read.csv("./inputData/StudentData.csv", header=True)
df.printSchema()

df.filter( df.marks > 50 ).show()
df.where(df.name.contains("se")).show()
#DSL operation - Domain Specific Language



print("=============== Filter from SQL ====================")
#sql format
df.createOrReplaceTempView("stundents")
spark.sql("select * from stundents where marks > 50").show()


print("=========== Multiple filter ==============")
df.filter( (df.course == "DB") & (df.marks > 60) ).show()


courses = ["Cloud", "OOP", "DSA"]

df.where( df.course.isin(courses) ).show()



print(df.filter(df.name.like('%se%')))

