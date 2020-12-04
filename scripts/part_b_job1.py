from mrjob.job import MRJob


class PartB_Job1(MRJob):
    def mapper(self, _, transaction):
        try:
            # split the block of transaction
            tsc = transaction.split(',')
            to_address = tsc[2]
            value = int(tsc[3])
            if len(tsc) == 7 and value != 0:
                yield(to_address, value)
        except:
            pass

    def combiner(self, to_address, values):
        yield (to_address, sum(values))

    def reducer(self, to_address, values):
        yield (to_address, sum(values))


if __name__ == "__main__":
    PartB_Job1.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartB_Job1.run()
