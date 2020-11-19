"""Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset."""

from mrjob.job import MRJob
import time

class EtherPartA1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch = int(fields[6])
                month = time.strftime("%b %Y", time.gmtime(time_epoch))
                yield month, 1
        except:
            pass

    def combiner(self, month, counts):
        yield month, sum(counts)

    def reducer(self, month, counts):
        yield month, sum(counts)


if __name__ == '__main__':
    EtherPartA1.run()
