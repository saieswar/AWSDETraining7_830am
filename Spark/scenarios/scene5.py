# walmart:  Histogram of Users and Purchases
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp, count, rank

# Initialize Spark session
spark = SparkSession.builder.appName("Transactions").getOrCreate()

# Define the schema
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("spend", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

# Create a list of tuples representing the data
data = [
    (3673, 123, 68.90, "07/08/2022 12:00:00"),
    (9623, 123, 274.10, "07/08/2022 12:00:00"),
    (1467, 115, 19.90, "07/08/2022 12:00:00"),
    (2513, 159, 25.00, "07/08/2022 12:00:00"),
    (1452, 159, 74.50, "07/10/2022 12:00:00")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Convert transaction_date to TimestampType
df = df.withColumn("transaction_date", to_timestamp(col("transaction_date"),"MM/dd/yyyy HH:mm:ss"))
df.createOrReplaceTempView("user_transactions")

df = df.groupBy("user_id","transaction_date").agg(count("product_id").alias("num_prod"))
df = df.withColumn("rank", rank().over(Window.partitionBy("user_id").orderBy(col("transaction_date").desc())))

df.select("user_id", "transaction_date", "num_prod").filter(df.rank==1).show()
# Show the DataFrame

spark.sql("""

with latest_transactions as ( 
  select 
    transaction_date,
    user_id,
    product_id,
    dense_rank() over (partition by user_id order by transaction_date desc) as latest_purchase
  from 
  user_transactions
)

select 
  transaction_date,
  user_id,
  sum(latest_purchase) as purchase_count 
from 
  latest_transactions 
where 
  latest_purchase = 1 
group by transaction_date, user_id 
order by transaction_date;


""").show()