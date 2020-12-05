from mrjob.job import MRJob
from mrjob.step import MRStep


class PartC(MRJob):
    def mapper_block_aggregate(self, _, mined_block):
        try:
            [block_num, _, miner, _, size] = mined_block.split(',')
            yield(block_num, (miner, int(size)))
        except:
            pass

    def combiner_block_aggregate(self, block_num, values):
        try:
            values = [x for x in values]
            data_types = {}
            for list in values:
                key = list[0]
                vals = list[1]
                if key in data_types:
                    data_types[key] = [sum(v)
                                       for v in zip(data_types[key], vals)]
                else:
                    data_types[key] = vals
            for data_type in data_types.items():
                yield(block_num, data_type)
        except:
            pass

    def reducer_block_aggregate(self, block_num, values):
        try:
            values = [x for x in values]
            data_types = {}
            for list in values:
                key = list[0]
                vals = list[1]
                if key in data_types:
                    data_types[key] = [sum(v)
                                       for v in zip(data_types[key], vals)]
                else:
                    data_types[key] = vals
            for data_type in data_types.items():
                yield(block_num, data_type)
        except:
            pass

    def mapper_size_aggregate(self, _, values):
        for value in values:
            # yield the miner as the key
            yield(value[0], value[1])

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
