from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
import json


class PartD(MRJob):
    eth_scams = {}

    def mapper_join_init(self):
        with open('scams.json') as f:
            parsed_json = json.loads(f.readline())
            scams_json = parsed_json["result"].items()
            for _, v in scams_json:
                category = v["category"]
                coin = v["coin"]
                addresses = v["addresses"]
                if coin == "ETH":
                    if category in self.eth_scams:
                        self.eth_scams[category] = list(
                            set(self.eth_scams[category] + addresses))
                    else:
                        self.eth_scams[category] = addresses
            f.close()

    def mapper_join(self, _, transaction):
        try:
            # split the block of transaction
            tsc = transaction.split(',')
            block_timestamp = int(tsc[-1])
            value = int(tsc[3])
            to_address = tsc[2]
            # convert the timestamp into a unified format of %month/%year (e.g. 10/2020)
            year_month_key = datetime.utcfromtimestamp(
                block_timestamp).strftime('%m/%Y')
            # yield the value with the count of 1
            yield(to_address, (year_month_key, value))
        except:
            pass

    def reducer_join(self, to_address, values):
        for k, v in self.eth_scams.items():
            if to_address in v:
                # yields (year_month_key, (category, value))
                yield(values[0], (k, values[1]))

    def mapper_category_aggregate(self, year_month_key, values):
        yield(year_month_key, values)

    def combiner_category_aggregate(self, year_month_key, values):
        values = [x for x in values]
        data_types = {}
        for arr in values:
            key = arr[0]
            vals = [arr[1]]
            if key in data_types:
                data_types[key] = [sum(v)
                                   for v in zip(data_types[key], vals)]
            else:
                data_types[key] = vals
        for data_type in data_types.items():
            yield(year_month_key, (data_type[0], data_type[1].pop()))

    def reducer_category_aggregate(self, year_month_key, values):
        values = [x for x in values]
        data_types = {}
        for arr in values:
            key = arr[0]
            vals = [arr[1]]
            if key in data_types:
                data_types[key] = [sum(v)
                                   for v in zip(data_types[key], vals)]
            else:
                data_types[key] = vals
        for data_type in data_types.items():
            yield(year_month_key, (data_type[0], data_type[1].pop()))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init, mapper=self.mapper_join, reducer=self.reducer_join), MRStep(mapper=self.mapper_category_aggregate, combiner=self.combiner_category_aggregate, reducer=self.reducer_category_aggregate)]


if __name__ == "__main__":
    PartD.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartD.run()
