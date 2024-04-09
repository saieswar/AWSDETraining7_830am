from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, when

spark = SparkSession.builder.appName("Student Grading").master("local").getOrCreate()
data1=[(1,"Steve"),(2,"David"),(3,"John"),(4,"Shree"),(5,"Helen")]
data2=[(1,"SQL",90),(1,"PySpark",100),(2,"SQL",70),(2,"PySpark",60),(3,"SQL",30),(3,"PySpark",20),(4,"SQL",50),(4,"PySpark",50),(5,"SQL",45),(5,"PySpark",45)]

schema1=["Id","Name"]
schema2=["Id","Subject","Mark"]

df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)
print("============== Data Frame 1 =====================")
df1.show()

print("============== Data Frame 2 =====================")
df2.show()
df2.printSchema()
print("=================== Joined DF =================")
df_join = df1.join(df2, df1.Id==df2.Id).drop(df2.Id)
df_join.show()

print("===================== Percentage ================")
df_per_student = df_join.groupBy("Id", "Name").agg(

    (sum("Mark")/count('*')).alias("Percentage")
)

df_per_student.show()

df_final = df_per_student.select("*",
                                    (when(df_per_student.Percentage >=70, "Distinction")
                                     .when( (df_per_student.Percentage <70) & (df_per_student.Percentage >=60), "First Class" )
                                     .when(  df_per_student.Percentage < 40, "Fail" )
                                 ).alias("Result")
                                 )
print("================ Final Result ==============")
df_final.show()