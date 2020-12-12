from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions
from pyspark.ml.regression import LinearRegression
from datetime import datetime

sc = SparkContext()
sparkSession = SparkSession.builder.getOrCreate()

# read the custom csv file from hdfs into the data frame
df = sparkSession.read.csv(
    "hdfs://andromeda.eecs.qmul.ac.uk/user/vcn01/input/eth_price_inception_to_11_dec.csv", header=True, inferSchema=False)

# get rid of all undefined values
clean_data = df.filter((df.open != "undefined") & (df.high != "undefined") & (
    df.low != "undefined") & (df.close != "undefined"))

# recast the columns as floats
clean_data = clean_data.withColumn(
    "timestamp", clean_data.timestamp.cast('bigint'))
clean_data = clean_data.withColumn("open", clean_data.open.cast('float'))
clean_data = clean_data.withColumn("high", clean_data.high.cast('float'))
clean_data = clean_data.withColumn("low", clean_data.low.cast('float'))
clean_data = clean_data.withColumn("close", clean_data.close.cast('float'))

# visual check that data are filtered
# clean_data.show()

# create features vector
feature_columns = clean_data.columns[:-1]  # here we omit the close column
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
transformed_data = assembler.transform(clean_data)

# train/test random split
train, test = transformed_data.randomSplit([0.7, 0.3])

# define the model
algo = LinearRegression(featuresCol="features", labelCol="close")

# train the model
model = algo.fit(train)

# evaluation
evaluation_summary = model.evaluate(test)
print("meanAbsoluteError: ", evaluation_summary.meanAbsoluteError)
print("rootMeanSquaredError: ", evaluation_summary.rootMeanSquaredError)
print("r2: ", evaluation_summary.r2)

# predicting values
predictions = model.transform(test)

# show the predictions
# predictions.show()

print("Job ID: ", sc.applicationId)

print("timestamp, open, high, low, close, features, prediction")
all_predictions = predictions.collect()
for p in all_predictions:
    formated_date = datetime.utcfromtimestamp(
        p[0]).strftime('%d/%m/%Y')
    print("{}, {}, {}, {}, {}, {}, {}".format(
        formated_date, p[1], p[2], p[3], p[4], p[5], p[6]))
