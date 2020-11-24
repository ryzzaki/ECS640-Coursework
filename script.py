from mrjob.job import MRJob


class Script(MRJob):
    def mapper(self, _, line):
        yield(line[0], None)

    def reducer(self, word, counts):
        yield(word, sum(counts))


if __name__ == "__main__":
    Script.run()
