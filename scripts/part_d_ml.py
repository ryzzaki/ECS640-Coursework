from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SparkSession
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
clean_data = clean_data.withColumn("timestamp", clean_data.open.cast('bigint'))
clean_data = clean_data.withColumn("open", clean_data.open.cast('float'))
clean_data = clean_data.withColumn("high", clean_data.open.cast('float'))
clean_data = clean_data.withColumn("low", clean_data.open.cast('float'))
clean_data = clean_data.withColumn("close", clean_data.open.cast('float'))

# visual check that data are filtered
clean_data.show()

# create features vector
feature_columns = clean_data.columns[:-1]  # here we omit the close column
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_2 = assembler.transform(clean_data)
# train/test split
train, test = data_2.randomSplit([0.7, 0.3])
# define the model
algo = LinearRegression(featuresCol="features", labelCol="close")
# train the model
model = algo.fit(train)
# evaluation
evaluation_summary = model.evaluate(test)
evaluation_summary.meanAbsoluteError
evaluation_summary.rootMeanSquaredError
evaluation_summary.r2
# predicting values
predictions = model.transform(test)
# here I am filtering out some columns just for the figure to fit
predictions.select(predictions.columns[4:]).show()
