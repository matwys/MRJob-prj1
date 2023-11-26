from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlight(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer
                   )
        ]

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport, destination_airport,
         scheduled_departure, departure_time, departure_delay, taxi_out, wheels_off, scheduled_time, elapsed_time,
         air_time, distance, wheels_on, taxi_in, scheduled_arrival, arrival_time, arrival_delay, diverted,
         cancelled, cancellation_reason, air_system_delay, security_delay, airline_delay, late_aircraft_delay,
         weather_delay) = line.split(',')
        if departure_delay == "":
            departure_delay = 0
        if arrival_delay == "":
            arrival_delay = 0
        departure_delay = float(departure_delay)
        arrival_delay = float(arrival_delay)
        yield airline, (departure_delay, arrival_delay)

    def combiner(self, key, values):
        total_departure_delay = 0
        total_arrival_delay = 0
        num_elements = 0
        for value in values:
            total_departure_delay += value[0]
            total_arrival_delay += value[1]
            num_elements += 1
        yield key, (total_departure_delay, total_arrival_delay, num_elements)

    def reducer(self, key, values):
        total_departure_delay = 0
        total_arrival_delay = 0
        num_elements = 0
        for value in values:
            total_departure_delay += value[0]
            total_arrival_delay += value[1]
            num_elements += value[2]
        yield key, (total_departure_delay/num_elements, total_arrival_delay/num_elements)

if __name__=='__main__':
    MRFlight.run()