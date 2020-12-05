from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType)
from filepaths.constants import ORDERS

spark = SparkSession.builder.appName("CustomerSpent").getOrCreate()

schema = StructType([
    StructField(name="customerID", dataType=IntegerType(), nullable=True),
    StructField(name="itemID", dataType=IntegerType(), nullable=True),
    StructField(name="amount", dataType=FloatType(), nullable=True)
])

# Read the file as dataframe
df = spark.read.schema(schema).csv(ORDERS)
df.printSchema()

# Total amount spent by each customer
customer_spent = (
    df
    .select("customerID", "amount")
    .groupBy("customerID")
    .agg(func.round(func.sum("amount"), 2).alias("amount_spent"))
    .sort("amount_spent", ascending=False)
)
customer_spent.show()

spark.stop()
