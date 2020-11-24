"""
This script will output the Month/Year key along with the tuple of
the [average value of transaction, number of transaction occurences].

The format is as follows:
"10/2016"	[1.8156913562048458e+18, 7]
"""

from mrjob.job import MRJob
from datetime import datetime


class PartA(MRJob):
    def mapper(self, _, transaction):
        try:
            # split the block of transaction
            tsc = transaction.split(',')
            block_timestamp = int(tsc[-1])
            value = int(tsc[3])
            # convert the timestamp into a unified format of %month/%year (e.g. 10/2020)
            year_month_key = datetime.utcfromtimestamp(
                block_timestamp).strftime('%m/%Y')
            # yield the value with the count of 1
            yield(year_month_key, (value, 1))
        except:
            pass

    def combiner(self, year_month_key, values):
        count = 0
        total = 0
        for value in values:
            total += value[0]
            count += value[1]
        yield (year_month_key, (total/count, count))

    def reducer(self, year_month_key, values):
        count = 0
        total = 0
        for value in values:
            total += value[0]
            count += value[1]
        yield (year_month_key, (total/count, count))


if __name__ == "__main__":
    PartA.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartA.run()
