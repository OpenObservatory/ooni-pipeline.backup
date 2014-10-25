import json
from pymongo import MongoClient
import sys
import pprint

def print_usage():
    print "Usage: export <bridge_hashes_filename> <output_filename>"
    sys.exit(-1)

def find_closest(controls, experiment):
    return min(controls, key=lambda x: abs(x['start_time'] - experiment['start_time']))

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

def get_experiment_measurements(country_measurements):
    # Experiments is a map from a country code to an experiment
    # measurement.
    experiments = {}
    for country, measurements in country_measurements.items():
        if country != 'NL':
            experiments[country] = measurements
    return experiments

def add_tcp_connect_field(measurement):
    if measurement['test_name'] == 'tcp_connect':
        measurement['tcp_

def add_status_field(measurement, controls):
    """ Iterate measurements and embed the status field."""
    if measurement['test_name'] == 'bridge_reachability':
        closest_control = find_closest(controls, measurement)
        status = truth_table(measurement, closest_control)
        measurement['status'] = status

def get_output(country_measurements):
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

    experiments = get_experiment_measurements(country_measurements)
    controls = country_measurements['NL']

    for country, measurements in experiments.items():
        if country not in output:
            output[country] = {}
        # For each experimental measurement find the corresponding control measurement
        # and compute the status field
        for measurement in measurements:
            add_status_field(measurement, controls)
            add_tcp_connect_field(measurement)

            # This is private data of mongodb
            del measurement['_id']
            del measurement['report_id']

            bridge = measurement['input']
            if bridge not in output[country]:
                output[country][bridge] = []
            output[country][bridge].append(measurement)

    return output


def main(hashes_filename, output_filename):
    hashes = get_hashes(hashes_filename)

    # Connect to database
    client = MongoClient('127.0.0.1', 27017)
    db = client.ooni

    # Find measurements we are interested in.
    measurements = db.measurements.find({"input": {"$in": hashes}})

    # Populate an auxiliary variable with the measurements of a report
    # report_ids is a map from a report_id to its [measurements]
    report_ids = {}
    for measurement in measurements:
        if not measurement['report_id'] in report_ids:
            report_ids[measurement['report_id']] = []
        report_ids[measurement['report_id']].append(measurement)

    country_measurements = {}
    for report_id, measurements in report_ids.items():
        report = db.reports.find_one({"_id": report_id})
        country = report['probe_cc']

        if country not in country_measurements:
            country_measurements[country] = []
        country_measurements[country].extend(measurements)

    output = get_output(country_measurements)
    with open(output_filename, 'w') as fp:
        json.dump(output, fp, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print_usage()
    hashes_filename = sys.argv[1]
    output_filename = sys.argv[2]
    main(hashes_filename, output_filename)
