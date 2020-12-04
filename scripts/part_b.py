from mrjob.job import MRJob


class PartB(MRJob):
    def mapper(self, _, transaction):
        try:
            # split the block of transaction
            tsc = transaction.split(',')
            to_address = tsc[2]
            value = tsc[3]
            if len(tsc) == 7:
                yield(to_address, value)
        except:
            pass

    def combiner(self, to_address, values):
        sorted_tsc_values = sorted(values, reverse=True, key=lambda val: val)
        i = 0
        for value in sorted_tsc_values:
            yield (to_address, value)
            i += 1
            if i >= 10:
                break

    def reducer(self, to_address, values):
        sorted_tsc_values = sorted(values, reverse=True, key=lambda val: val)
        i = 0
        for value in sorted_tsc_values:
            yield (to_address, value)
            i += 1
            if i >= 10:
                break


if __name__ == "__main__":
    PartB.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartB.run()
