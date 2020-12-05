from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from filepaths.constants import REALESTATE


# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("DecisionTrees").getOrCreate()

# Load up our data and convert it to the format MLLib expects.
data = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(REALESTATE)
)

assembler = (
    VectorAssembler()
    .setInputCols([
        "HouseAge", "DistanceToMRT", "NumberConvenienceStores"
    ])
    .setOutputCol("features")
)

df = assembler.transform(data).select("PriceOfUnitArea", "features")

# Let's split our data into training data and testing data
trainTest = df.randomSplit([0.5, 0.5])
trainingDF = trainTest[0]
testDF = trainTest[1]

# Now create our linear regression model
tree = (
    DecisionTreeRegressor()
    .setFeaturesCol("features")
    .setLabelCol("PriceOfUnitArea")
)

# Train the model using our training data
model = tree.fit(trainingDF)

# Now see if we can predict values in our test data.
# Generate predictions using our linear regression model for all features in
# our test dataframe:
fullPredictions = model.transform(testDF).cache()

# Extract the predictions and the "known" correct labels.
predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

# Zip them together
predictionAndLabel = predictions.zip(labels).collect()

# Print out the predicted and actual values for each point
for prediction in predictionAndLabel:
    print(prediction)

# Stop the session
spark.stop()
