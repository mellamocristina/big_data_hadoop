from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    def configure_args(self):
        super(MRFlights, self).configure_args()
        self.add_file_arg('--airlines', help='Path to the airlines.csv')

    def mapper(self, _, line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT,
         SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
         AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED,
         CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY,
         WEATHER_DELAY) = line.split(',')

        # sprawdzamy dlaczego dane nie chcą się przekonwertować na float
        #try:
        #    DEPARTURE_DELAY = float(DEPARTURE_DELAY)
        #except:
        #    yield None, DEPARTURE_DELAY
        #    # ARRIVAL_DELAY = float(ARRIVAL_DELAY)

        if DEPARTURE_DELAY == '':
            DEPARTURE_DELAY = 0

        if ARRIVAL_DELAY == '':
            ARRIVAL_DELAY = 0

        DEPARTURE_DELAY = float(DEPARTURE_DELAY)
        ARRIVAL_DELAY = float(ARRIVAL_DELAY)


        yield AIRLINE, (DEPARTURE_DELAY, ARRIVAL_DELAY)


    def reducer_init(self):
        self.airline_names = {}

        with open('airlines.csv', 'r') as file:
            for line in file:
                items = line.split(',')
                code, full_name = line.split(',')
                full_name = full_name[:-1]  # wycinamy ostatni znak (konca linii \n)
                self.airline_names[code] = full_name

    def reducer(self, AIRLINE, values):
        total_dep_delay = 0
        total_arr_delay = 0
        num_elements = 0
        for value in values:
            total_dep_delay += value[0]
            total_arr_delay += value[1]
            num_elements += 1
        yield self.airline_names[AIRLINE], (total_dep_delay/num_elements, total_arr_delay/num_elements)


if __name__ == '__main__':
    MRFlights.run()

# python 06_average_departure_arrival_delay_by_airlines_with_names.py flights.csv --airlines airlines.csv
