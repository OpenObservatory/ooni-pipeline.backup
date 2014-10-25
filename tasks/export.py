import json
from pymongo import MongoClient
import sys
from measurements import Measurement, Measurements

def print_usage():
    print "Usage: export <bridge_hashes_filename> <output_filename>"
    sys.exit(-1)


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
