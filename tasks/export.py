import json
from pymongo import MongoClient
import sys
import pprint

class Measurement(object):
    def __init__(self, measurement, mongodb_client):
        self.measurement = measurement
        self.report_id = measurement['report_id']
        self.mongodb_client = mongodb_client
        self.report = self.mongodb_client.reports.find_one({"_id": self.report_id})

    def get_test_name(self):
        return self.report['test_name']

    def get_country(self):
        return self.report['probe_cc']

    def is_bridge_reachability(self):
        return self.report['test_name'] == 'bridge_reachability'

    def scrub(self):
        # This is private data of mongodb
        del self.measurement['_id']
        del self.measurement['report_id']

    def add_status_field(self, controls):
        """ Iterate measurements and embed the status field."""
        closest_control = find_closest(controls, self.measurement)
        status = truth_table(self.measurement, closest_control.measurement)
        self.measurement['status'] = status

    def add_tcp_connect_field(measurement):
        pass

class Measurements(object):
    def __init__(self, measurements, db):
        self.measurements = []
        self.db = db
        for measurement in measurements:
            self.add_measurement(measurement)

    def add_measurement(self, measurement):
        self.measurements.append(Measurement(measurement, self.db))

    def get_experiments(self):
        # Experiments is a map from a country code to an experiment
        # measurement.
        experiments = {}
        for measurement in self.measurements:
            if not measurement.is_bridge_reachability():
                continue

            country = measurement.get_country()
            if country != 'NL':
                if country not in experiments:
                    experiments[country] = []
                experiments[country].append(measurement)
        return experiments

    def get_controls_list(self):
        controls = []
        for measurement in self.measurements:
            if not measurement.is_bridge_reachability():
                continue

            country = measurement.get_country()
            if country == 'NL':
                controls.append(measurement)
        return controls

    def __iter__(self):
        return self

    def next(self):
        for measurement in self.measurements:
            yield measurement
        raise StopIteration

def print_usage():
    print "Usage: export <bridge_hashes_filename> <output_filename>"
    sys.exit(-1)

def find_closest(controls, experiment):
    start_time = experiment['start_time']
    return min(controls, key=lambda x: abs(x.measurement['start_time'] - start_time))

def truth_table(experiment, control):
    result_experiment = experiment['success']
    result_control = control['success']

    if result_experiment == True and result_control == True:
        return "ok"
    elif result_experiment == True and result_control == False:
        return "inconsistent"
    elif result_experiment == False and result_control == True:
        return "blocked"
    elif result_experiment == False and result_control == False:
        return "offline"

def get_hashes(hashes_filename):
    """ Get hashes from filename input"""
    hashes = open(hashes_filename).readlines()
    hashes = [h.rstrip() for h in hashes]
    return hashes

def get_output(measurements):
    """
    Generate the output

    'output' is a map from country codes to 'bridge_dictionaries'.
    'bridge_dictionaries' is a map from bridges to their measurements.
    Example:
    {"RU" : { "1.2.3.4:42040" : [ {"transport_name" ...}, {"transport_name ..."} ],
            { "4.3.2.1:60465" : [ {"transport_name" ...}, {"transport_name ..."} ],
     "CN" : { ...}
    """
    output = {}

    experiments = measurements.get_experiments()
    controls = measurements.get_controls_list()

    for country, measurements in experiments.items():
        if country not in output:
            output[country] = {}
        # For each experimental measurement find the corresponding 
        # control measurement and compute the status field
        for measurement in measurements:
            measurement.add_status_field(controls)
            measurement.add_tcp_connect_field()

            measurement.scrub()

            bridge = measurement.measurement['input']
            if bridge not in output[country]:
                output[country][bridge] = []
            output[country][bridge].append(measurement.measurement)

    return output


def main(hashes_filename, output_filename):
    hashes = get_hashes(hashes_filename)

    # Connect to database
    client = MongoClient('127.0.0.1', 27017)
    db = client.ooni

    # Find measurements we are interested in.
    ms = db.measurements.find({"input": {"$in": hashes}})

    measurements = Measurements(ms, db)

    output = get_output(measurements)
    with open(output_filename, 'w') as fp:
        json.dump(output, fp, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print_usage()
    hashes_filename = sys.argv[1]
    output_filename = sys.argv[2]
    main(hashes_filename, output_filename)
