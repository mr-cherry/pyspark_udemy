from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from filepaths.constants import MARVEL_NAME, MARVEL_GRAPH

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(MARVEL_NAME)

lines = spark.read.text(MARVEL_GRAPH)

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = connections.agg(func.min("connections")).first()[0]

minConnections = connections.filter(func.col("connections") == minConnectionCount)

minConnectionsWithNames = minConnections.join(names, "id")

print("The following characters have only " + str(minConnectionCount) + " connection(s):")

minConnectionsWithNames.select("name").show()
