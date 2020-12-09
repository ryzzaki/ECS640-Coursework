from mrjob.job import MRJob
from mrjob.step import MRStep


class PartC(MRJob):
    def mapper_block_aggregate(self, _, mined_block):
        try:
            splits = mined_block.split(',')
            block_num = str(splits[0])
            miner = splits[2]
            size = int(splits[4])
            if block_num.isnumeric() is True:
                yield(block_num, (miner, size))
        except:
            pass

    def combiner_block_aggregate(self, block_num, values):
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
            yield(block_num, (data_type[0], data_type[1].pop()))

    def reducer_block_aggregate(self, block_num, values):
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
            yield(block_num, (data_type[0], data_type[1].pop()))

    def mapper_size_aggregate(self, _, values):
        values = [x for x in values]
        yield(values[0], values[1])

    def combiner_size_aggregate(self, miner, sizes):
        yield(miner, sum(sizes))

    def reducer_size_aggregate(self, miner, sizes):
        yield(miner, sum(sizes))

    def mapper_address_aggregate(self, miner, size):
        yield(None, (miner, size))

    def reducer_address_aggregate(self, _, values):
        i = 0
        sorted_values = sorted(values, reverse=True, key=lambda tup: tup[1])
        for value in sorted_values:
            i += 1
            yield("{} - {} - {}".format(i, value[0], value[1]), None)
            if i >= 10:
                break

    def steps(self):
        return [MRStep(mapper=self.mapper_block_aggregate,
                       combiner=self.combiner_block_aggregate, reducer=self.reducer_block_aggregate),
                MRStep(mapper=self.mapper_size_aggregate,
                       combiner=self.combiner_size_aggregate, reducer=self.reducer_size_aggregate),
                MRStep(mapper=self.mapper_address_aggregate, reducer=self.reducer_address_aggregate)]


if __name__ == "__main__":
    PartC.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartC.run()
