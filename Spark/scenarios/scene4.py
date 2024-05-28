from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, col, desc

spark = SparkSession.builder.master("local").appName("jpmorgan").getOrCreate()


data = [
    (1, 2021, "Chase Sapphire Reserve", 170000),
    (2, 2021, "Chase Sapphire Reserve", 175000),
    (3, 2021, "Chase Sapphire Reserve", 180000),
    (3, 2021, "Chase Freedom Flex", 65000),
    (4, 2021, "Chase Freedom Flex", 70000)
]


# Define the schema
schema = ["issue_month", "issue_year", "card_name", "issued_amount"]

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView("monthly_cards_issued")
windDF = Window.partitionBy("card_name").orderBy(["issue_year", "issue_month"])
# Show the DataFrame
df.show()
df.withColumn("card_rank",rank().over(windDF)).filter(col("card_rank")==1).orderBy(df.issued_amount.desc()).select("card_name","issued_amount").show()
spark.sql("""

with rank_table as (

select card_name,
        issued_amount,
        issue_year,
        issue_month,
        rank() over(partition by card_name order by issue_year, issue_month) as raank
    from monthly_cards_issued
)

select card_name,
        issued_amount
    from rank_table
    where raank=1 
    order by issued_amount desc;

""").show()