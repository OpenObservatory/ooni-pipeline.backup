""" Preprocess M-Lab files converting them in .yamloo files """

# TODO: adapt this code to process all M-Lab files rather than only Glasnost

import StringIO
import os
import re
import tarfile
import yaml

from ooni.pipeline.preprocess import glasnost
from ooni.pipeline import settings

MLAB_FILE_PATTERN = \
"^[0-9]{8}T[0]{6}Z-mlab[1-9]{1}-[a-z]{3}[0-9]{2}-[a-z]+-[0-9]{4}.tgz$"


def list_report_files(directory):
    """ Lists the report files to process """
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if re.match(MLAB_FILE_PATTERN, filename):
                yield os.path.join(dirpath, filename)


def process_glasnost_log(pathname, pseudofile):
    """ Process the log of a Glasnost test """

    sio = StringIO.StringIO(pseudofile.read())
    test_info = glasnost.preparser(pathname, sio)
    if not test_info:
        return

    if test_info["proto"] == "BitTorrent (v1-log)":
        return  # Not supported
    if not test_info["proto"]:
        return
    if not test_info["done"]:
        return

    measurements, stats = glasnost.parse_summary_string_log2(
        test_info["client_sum"], test_info["server_sum"])

    result, warnings, interim = glasnost.glasnost_analysis_v2(measurements)

    report_header = {
        "options": {},
        "probe_asn": None, #TODO
        "probe_cc": None, # TODO
        "probe_ip": test_info["client_ip"],
        "software_name": "Glasnost",
        "version": "v2",
        "start_time": test_info["start_timestamp"],
        "test_name": test_info["proto"],
        "test_version": "v2",
        "data_format_version": "v2",
    }

    import sys  # XXX
    yaml.safe_dump(report_header, sys.stdout, explicit_start=True,
                   explicit_end=True, default_flow_style=False)

    report = {
        "test_info": test_info,
        "measurements": [],
        "result": result,
        "warnings": warnings,
        "interim": interim
    }
    for measurement in measurements:
        report["measurements"].append(measurement.to_dict())

    import sys  # XXX
    yaml.safe_dump(report, sys.stdout, explicit_start=True,
                   explicit_end=True, default_flow_style=False)

def process_glasnost_tarball(filepath):
    """ Process a single glasnost tarball """
    tarball = tarfile.open(filepath, "r")
    for member in tarball:
        if member.isreg():
            if member.path.endswith(".log"):
                pseudofile = tarball.extractfile(member)
                process_glasnost_log(member.path, pseudofile)


def main():
    """ Main function """
    for filepath in list_report_files(settings.raw_directory):
        process_glasnost_tarball(filepath)
