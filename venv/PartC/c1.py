from mrjob.job import MRJob


class TopMiners(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                miner = fields[2]
                size = float(fields[7]) * float(fields[6])
                yield (None, (miner, size))
        except :
            pass

    def combiner(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[2])
        i = 0
        for value in sorted_values:
            yield ("{} - {}".format(value[0],value[1]),None)
            i += 1
            if i >= 10:
                break


if __name__ == '__main__':
    TopMiners.run()
