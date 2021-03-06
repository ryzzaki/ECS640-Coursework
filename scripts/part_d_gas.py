from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class PartD(MRJob):
    def reduce_values_by_local_key(self, values):
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
        return data_types

    def mapper_address_aggregate(self, _, row):
        try:
            splits = row.split(',')
            if len(splits) == 7:
                # this is a transaction
                to_address = splits[2]
                gas = int(splits[4])
                gas_price = int(splits[5])
                tsc_cost = gas * gas_price
                block_timestamp = int(splits[-1])
                # convert the timestamp into a unified format of %month/%year (e.g. 10/2020)
                year_month_key = datetime.utcfromtimestamp(
                    block_timestamp).strftime('%m/%Y')
                if tsc_cost > 0:
                    yield(to_address, (year_month_key, tsc_cost))
            elif len(splits) == 5:
                # this is a smart contract
                sc_address = splits[0]
                yield(sc_address, ("sc", 1))
            else:
                pass
        except:
            pass

    def combiner_address_aggregate(self, address, values):
        data_types = self.reduce_values_by_local_key(values)
        for data_type in data_types.items():
            yield(address, (data_type[0], data_type[1].pop()))

    def reducer_address_aggregate(self, address, values):
        has_sc = False
        values = [x for x in values]
        # loop through the values and count the transacted amounts in smart contracts
        for value in values:
            if value[0] == "sc":
                has_sc = True
                values.remove(value)
        data_types = self.reduce_values_by_local_key(values)
        # only yield if this is a smart contract
        if has_sc is True:
            for data_type in data_types.items():
                if data_type[0] != "sc":
                    yield(data_type[0], data_type[1].pop())

    def mapper_month_aggregate(self, year_month_key, tsc_cost):
        yield(year_month_key, tsc_cost)

    def combiner_month_aggregate(self, year_month_key, tsc_cost):
        yield(year_month_key, sum(tsc_cost))

    def reducer_month_aggregate(self, year_month_key, tsc_cost):
        yield(year_month_key, sum(tsc_cost))

    def steps(self):
        return [MRStep(mapper=self.mapper_address_aggregate, combiner=self.combiner_address_aggregate,
                       reducer=self.reducer_address_aggregate),
                MRStep(mapper=self.mapper_month_aggregate, combiner=self.combiner_month_aggregate,
                       reducer=self.reducer_month_aggregate)]


if __name__ == "__main__":
    PartD.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartD.run()
