from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from filepaths.constants import FRIENDS_HEAD

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(FRIENDS_HEAD)
)

print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().orderBy("count", ascending=False).show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

print("Average number of friends by age:")
(
    people
    .select("age", "friends")
    .groupBy("age")
    .agg(func.round(func.avg("friends"), 2).alias("friends_avg"))
    .orderBy("age")
    .show()
)


spark.stop()
