import json
from pymongo import MongoClient
import sys
import pprint

def print_usage():
    print "Usage: export <bridge_hashes_filename> <output_filename>"

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

def get_output(experiments, controls):
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

    for country, measurements in experiments.items():
        if country not in output:
            output[country] = {}
        # For each experimental measurement find the corresponding control measurement
        # and compute the status field
        for measurement in measurements:
            closest_control = find_closest(controls, measurement)
            status = truth_table(measurement, closest_control)
            measurement['status'] = status

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
    measurements = db.measurements.aggregate([{"$match": {"input": {"$in": hashes}}}])

    # Populate an auxiliary variable with the measurements of a report
    # report_ids is a map from a report_id to its [measurements]
    report_ids = {}
    for measurement in measurements['result']:
        if not measurement['report_id'] in report_ids:
            report_ids[measurement['report_id']] = []
        report_ids[measurement['report_id']].append(measurement)

    # For each report, assign it to be a control or experiment.

    # Controls is a list of control measurements
    controls = []
    # Experiments is a map from a country code to an experiment
    # measurement.
    experiments = {}
    for report_id, measurements in report_ids.items():
        report = db.reports.find_one({"_id": report_id})
        country = report['probe_cc']

        # If country is the Netherlands, it's a control measurement.
        if country == 'NL':
            controls.extend(measurements)
        else:
            if country not in experiments:
                experiments[country] = []
            experiments[country].extend(measurements)

    output = get_output(experiments, controls)
    with open(output_filename, 'w') as fp:
        json.dump(output, fp, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print_usage()
        sys.exit(-1)
    hashes_filename = sys.argv[1]
    output_filename = sys.argv[2]
    output = main(hashes_filename, output_filename)
