# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 15:28:00 2020

@author: Frank
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs
from filepaths.constants import MOVIE_DATA, MOVIE_ITEM_SHORT


def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open(MOVIE_ITEM_SHORT, "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv(MOVIE_DATA)

movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to look up movie names
# from our broadcasted dictionary
lookupNameUDF = func.udf(lambda movieID: nameDict.value[movieID])

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn(
    "movieTitle",
    lookupNameUDF(movieCounts.movieID)
)

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy("count", ascending=False)

# Grab the top 10
sortedMoviesWithNames.show(10, truncate=False)

# Stop the session
spark.stop()
