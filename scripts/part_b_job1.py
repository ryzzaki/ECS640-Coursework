from mrjob.job import MRJob


class PartB_Job1(MRJob):
    def mapper(self, _, row):
        try:
            splits = row.split(',')
            if len(splits) == 7:
                to_address = splits[2]
                value = int(splits[3])
                if value > 0:
                    yield(to_address, value)
        except:
            pass

    def combiner(self, address, values):
        yield(address, sum(values))

    def reducer(self, address, values):
        yield(address, sum(values))


if __name__ == "__main__":
    PartB_Job1.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartB_Job1.run()
