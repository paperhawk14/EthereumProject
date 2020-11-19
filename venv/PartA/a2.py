"""Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

here we want to take the values for all the transactions for a month and add them up, then devide by the total number
of transactions that month

don't need to do fancy things, just the total amount for that month as I already have the amount of transactions from part1"""

from mrjob.job import MRJob
import time


class EtherPartA2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%b %Y", time.gmtime(time_epoch))
                amount = float(fields[4]*fields[5])
                yield None, (month, amount)
        except:
            pass

    def reducer(self, month, amounts):
        total = 0
        yield (month, sum(amounts))

if __name__ == '__main__':
    EtherPartA2.run()
