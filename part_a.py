from mrjob.job import MRJob
from datetime import datetime


class PartA(MRJob):
    def mapper(self, _, transaction):
        # split the block of transaction and select timestamp
        block_timestamp = transaction.split(',')[-1]
        # convert the timestamp into a unified format of %month-%year (e.g. 10-2020)
        year_month_key = datetime.utcfromtimestamp(
            int(block_timestamp)).strftime('%m-%Y')
        # yield the result with the count of 1
        yield(year_month_key, 1)

    def reducer(self, year_month_key, counts):
        yield(year_month_key, sum(counts))


if __name__ == "__main__":
    PartA.JOBCONF = {'mapreduce.job.reduces': '10'}
    PartA.run()
