import scipy.stats as sps
import time

def transfer_confidence_simple(arrival_delay, time_budget):
    if arrival_delay <= 0:
        return 1.0
    return sps.expon.cdf(time_budget, scale=arrival_delay)

def time_from_string(datetime_str):
    time_str = datetime_str
    h, m, s = map(int, time_str.split(':'))
    return h * 3600 + m * 60 + s

def time_diff(time1: float, time2: float):
    if time1 - time2 < 0:
        return time1 - time2 + 24 * 3600
    return time1 - time2

def journey_confidence_on_arrival_delay_predictions(journey, arrival_delay_predictions, journey_arrival_datetime, stops_matching):
    arrival_time_last = None
    last_arrival_delay = None
    walking_time = 0
    confidence = 1.0
    eps = 60

    fixed_arrival_delay_predictions = []
    for i in range(len(journey)-1):
        if stops_matching[journey[i]['arrival_stop']] in arrival_delay_predictions.keys() and journey[i]['transport'] != 'walking':
            fixed_arrival_delay_predictions.append(arrival_delay_predictions[stops_matching[journey[i]['arrival_stop']]])
        else:
            fixed_arrival_delay_predictions.append(0 if journey[i]['transport'] == 'walking' else eps)

    journey_arrival_time_in_secs = time_from_string(journey_arrival_datetime)

    for leg, delay in zip(journey[0:-1], fixed_arrival_delay_predictions):
        start_time = leg['start_time']
        end_time = leg['arrival_time']
        if leg['transport'] == 'walking':
            walking_time += time_diff(end_time, start_time)
        else:
            if arrival_time_last is not None:
                time_budget = time_diff(start_time, arrival_time_last) - walking_time
                if time_budget > 0:
                    confidence *= transfer_confidence_simple(
                        last_arrival_delay,
                        time_budget
                    )
            arrival_time_last = end_time
            last_arrival_delay = delay
            walking_time = 0
    
    # Probability that the last train arrives on time required
    if arrival_time_last is not None:
        time_budget = time_diff(journey_arrival_time_in_secs, arrival_time_last) - walking_time
        if time_budget > 0:
            confidence *= transfer_confidence_simple(
                last_arrival_delay,
                time_budget
            )

    return confidence
