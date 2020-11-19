"""Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

here we want to take the values for all the transactions for a month and add them up, then devide by the total number
of transactions that month"""

from mrjob.job import MRJob
import time


class EtherPartA2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%d %Y", time.gmtime(time_epoch))
                amount = fields[4]*fields[5]
                yield ('Transactions', month, 1)
                yield ('Amounts', amount, 1)
        except:
            pass

    def combiner(self, feature, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield (feature, (total, count) )


    def reducer(self, feature, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield (feature, total/count)

if __name__ == '__main__':
    EtherPartA2.run()