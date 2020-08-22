from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        YEAR, items = line.split('\t')
        YEAR = YEAR[1:-1]
        items = items[1:-1]
        MONTH, DAY, AIRLINE, DISTANCE = items.split(', ')
        DISTANCE = int(DISTANCE)
        yield None, DISTANCE

    def reducer(self, key, values):
        total = 0
        num_elements = 0

        for value in values:
            total += value
            num_elements += 1
        yield None, total/num_elements


if __name__ == '__main__':
    MRFlights.run()