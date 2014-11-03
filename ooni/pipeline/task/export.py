import os
import sys
import json
from ooni.pipeline import settings
from ooni.pipeline.measurements import Measurements


def get_hashes(bridge_db_filename):
    """ Get hashes from filename input"""
    with open(bridge_db_filename) as f:
        bridge_db = json.load(f)
    hashes = []
    for ip, value in bridge_db.items():
        hashes.append(value['hashed_fingerprint'])
    return hashes


def generate_bridges_by_country_code(measurements):
    """
    Generate the output

    'output' is a map from country codes to 'bridge_dictionaries'.
    'bridge_dictionaries' is a map from bridges to their measurements.
    Example:
    {"RU" : { "1.2.3.4:42040" : [ {"transport_name" ...},
                                  {"transport_name ..."} ],
            { "4.3.2.1:60465" : [ {"transport_name" ...},
                                  {"transport_name ..."} ],
     "CN" : { ...}
    """
    output = {}

    experiments = measurements.get_experiments()
    controls = measurements.get_controls_list()

    tcp_connects = measurements.get_tcp_connects()

    for country, measurements in experiments.items():
        print("[+] Looking at %s" % country)
        if country not in output:
            output[country] = {}
        # For each experimental measurement find the corresponding
        # control measurement and compute the status field
        for measurement in measurements:
            measurement.add_status_field(controls)
            measurement.add_start_time()
            measurement.add_file_url()
            if measurement.add_tcp_connect_field(tcp_connects):
                sys.stdout.write(".")
            else:
                sys.stdout.write("x")
            sys.stdout.flush()

            measurement.scrub()

            bridge = measurement.measurement['input']
            if bridge not in output[country]:
                output[country][bridge] = []
            output[country][bridge].append(measurement.measurement)
        sys.stdout.write("\n")
        print("[*] done.")

    return output

def generate_summary(bridges_by_country_code):
    summary = {}

    for country, bridge_by_hash in bridges_by_country_code.items():
        transport_hash = {}
        for bridge_hash, measurements in bridge_by_hash.items():
            # The transport will always be the same
            transport_name = measurements[0]["transport_name"]
            if 'transport_name' not in transport_hash:
                transport_hash[transport_name] = []
            d = {}
            #d[bridge_hash] =
            transport_hash[transport_name].append(d)

        summary[country] = transport_hash

def main(bridge_db_filename, output_filename):
    # summary_file = os.path.join(os.path.basename(output_filename),
    #                             "summary.json")

    hashes = get_hashes(bridge_db_filename)

    # Find measurements we are interested in.
    ms = settings.db.measurements.find({"input": {"$in": hashes}})
    measurements = Measurements(ms, settings.db)

    bridges_by_country_code = generate_bridges_by_country_code(measurements)
    with open(output_filename, 'w') as fp:
        json.dump(bridges_by_country_code, fp, sort_keys=True, indent=4, separators=(',', ': '))
    # summary = generate_summary(bridges_by_country_code)
    # with open(summary_file, 'w') as fp:
    #     json.dump(summary, fp, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    main(settings.bridge_db_filename, settings.bridge_by_country_code_output)
