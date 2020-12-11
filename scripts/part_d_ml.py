import pyspark
from datetime import datetime

sc = pyspark.SparkContext()


def is_good_tsc_line(line):
    try:
        splits = line.split(',')
        if len(splits) != 7:
            return False
        # tsc value
        float(splits[3])
        # timestamp
        float(splits[-1])
        return True
    except:
        return False


def is_good_price_line(line):
    try:
        splits = line.split(',')
        if len(splits) != 5:
            return False
        # timestamp
        int(splits[0])
        # open price
        int(splits[1])
        return True
    except:
        return False


def mapper(line):
    splits = line.split(',')
    if len(splits) == 7:
        value = int(splits[3])
        block_timestamp = splits[-1]
        date = datetime.utcfromtimestamp(
            block_timestamp).strftime('%d/%m/%Y')
    else:
        value = int(splits[1])
        block_timestamp = splits[0]
        date = datetime.utcfromtimestamp(
            block_timestamp).strftime('%d/%m/%Y')
    return (date, value)


# import the transactions and prices
transactions = sc.textFile("/data/ethereum/transactions/")
prices = sc.textFile("input/eth_price_inception_to_11_dec.csv")

# clean the dataset
clean_tsc_lines = transactions.filter(is_good_tsc_line)
clean_prices_lines = prices.filter(is_good_price_line)

# map & reduce
dates_and_values = clean_tsc_lines.map(mapper).reduceByKey(lambda a: sum(a))
dates_and_prices = clean_prices_lines.map(mapper).reduceByKey(lambda a: sum(a))

# join => (date, (value, price))
joined_dates = dates_and_values.join(dates_and_prices)

# linearModel = LinearRegressionWithSGD.train(joined_dates, 1000, .2)
# or use pyspark.ml.regression.LinearRegression()?
