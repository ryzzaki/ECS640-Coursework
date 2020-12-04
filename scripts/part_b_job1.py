from mrjob.job import MRJob


class PartB_Job1(MRJob):
    def mapper(self, _, row):
        try:
            splits = row.split(',')
            if len(splits) == 7:
                # this is a transaction
                to_address = splits[2]
                value = int(splits[3])
                if value != 0:
                    yield(to_address, ["tsc", value])
            elif len(splits) == 5:
                # this is a contract
                sc_address = splits[0]
                is_erc20 = splits[1]
                is_erc721 = splits[2]
                yield(sc_address, ["sc", is_erc20, is_erc721])
            else:
                pass
        except:
            pass

    def combiner(self, address, values):
        values = [x for x in values]
        if len(values):
            value_type = values[0]
            for value in values[1:]:
                yield(address, [value_type, value])
        else:
            pass

    def reducer(self, address, values):
        values = [x for x in values]
        if len(values):
            value_type = values[0]
            for value in values[1:]:
                yield(address, [value_type, value])
        else:
            pass


if __name__ == "__main__":
    PartB_Job1.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartB_Job1.run()
