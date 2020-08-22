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
        MONTH = int(MONTH)

        yield f'{MONTH:02d}', (DEPARTURE_DELAY, ARRIVAL_DELAY)

    def reducer(self, MONTH, values):
        total_dep_delay = 0
        total_arr_delay = 0
        num_elements = 0
        for value in values:
            total_dep_delay += value[0]
            total_arr_delay += value[1]
            num_elements += 1
        yield MONTH, (total_dep_delay/num_elements, total_arr_delay/num_elements)


if __name__ == '__main__':
    MRFlights.run()

# 04_average_departure_arrival_delay_by_month