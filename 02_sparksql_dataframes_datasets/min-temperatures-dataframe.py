from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType)
from filepaths.constants import TEMPS

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([
    StructField(name="stationID", dataType=StringType(), nullable=True),
    StructField(name="date", dataType=IntegerType(), nullable=True),
    StructField(name="measure_type", dataType=StringType(), nullable=True),
    StructField(name="temperature", dataType=FloatType(), nullable=True)
])

# Read the file as dataframe
df = spark.read.schema(schema).csv(TEMPS)
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = (
    stationTemps
    .groupBy("stationID")
    .agg(func.min("temperature").alias("temp_min"))
)
minTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
minTempsByStationF = (
    minTempsByStation
    .withColumn(
        "temperature",
        func.round(func.col("temp_min") * 0.1 * (9.0 / 5.0) + 32.0, 2)
        )
    .select("stationID", "temperature")
    .sort("temperature")
)

# Collect, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
