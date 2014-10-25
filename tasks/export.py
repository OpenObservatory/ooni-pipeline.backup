import os
import json
from pymongo import MongoClient
from measurements import Measurements

db_ip, db_port = os.environ['OONI_DB_IP'], int(os.environ['OONI_DB_PORT'])


def get_hashes(bridge_db_filename):
    """ Get hashes from filename input"""
    with open(bridge_db_filename) as f:
        bridge_db = json.loads(f)
    hashes = []
    for ip, value in bridge_db.items():
        hashes.append(value['hashed_fingerprint'])
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


def main(bridge_db_filename, output_filename):
    hashes = get_hashes(bridge_db_filename)

    # Connect to database
    client = MongoClient(db_ip, db_port)
    db = client.ooni

    # Find measurements we are interested in.
    ms = db.measurements.find({"input": {"$in": hashes}})

    measurements = Measurements(ms, db)

    output = get_output(measurements)
    with open(output_filename, 'w') as fp:
        json.dump(output, fp, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    bridge_db_filename = os.environ['OONI_BRIDGE_DB_FILE']
    output_filename = os.path.join(os.environ['OONI_PUBLIC_DIR'],
                                   'bridges-by-country-code.json')
    main(bridge_db_filename, output_filename)
