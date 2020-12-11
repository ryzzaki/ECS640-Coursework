from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from datetime import datetime

sc = SparkContext()
sparkSession = SparkSession.builder.getOrCreate()


def is_good_line(line):
    try:
        splits = line.split(',')
        if len(splits) != 5:
            return False
        float(splits[1])
        float(splits[2])
        float(splits[3])
        float(splits[4])
        return True
    except:
        return False


data = sparkSession.read.csv(
    "hdfs://andromeda.eecs.qmul.ac.uk/user/vcn01/input/eth_price_inception_to_11_dec.csv", header=True, inferSchema=True)

clean_data = data.filter(is_good_line)

# create features vector
feature_columns = clean_data.columns[:-1]  # here we omit the final column
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_2 = assembler.transform(clean_data)
# train/test split
train, test = data_2.randomSplit([0.7, 0.3])
# define the model
algo = LinearRegression(featuresCol="features", labelCol="open_prices")
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
predictions.select(predictions.columns[13:]).show()
