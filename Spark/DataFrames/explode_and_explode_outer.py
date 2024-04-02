from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ("James",['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})
    ]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

df1 =df.select(df.name, explode(df.knownLanguages))
df1.printSchema()
df1.show()
print("==========explode on dict=====================")
df2 = df.select(df.name,explode(df.properties))
df2.show()









print("=============Explode Outer===============")

df.select(df.name,explode_outer(df.knownLanguages)).show()
df.select(df.name,explode_outer(df.properties)).show()





