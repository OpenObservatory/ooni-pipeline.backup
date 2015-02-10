""" Preprocess M-Lab files converting them in .yamloo files """

# TODO: adapt this code to process all M-Lab files rather than only Glasnost

import StringIO
import datetime
import os
import re
import tarfile
import yaml

from ooni.pipeline.preprocess import glasnost
from ooni.pipeline.preprocess import lookup_probe_asn
from ooni.pipeline.preprocess import lookup_geoip
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

    geoinfo = lookup_geoip(settings.geoip_directory, test_info["client_ip"],
                           test_info["start_timestamp"])

    if geoinfo and geoinfo["country_code"]:
        country_code = geoinfo["country_code"].decode("iso-8859-1")
    else:
        country_code = None
    if geoinfo and geoinfo["city"]:
        city = geoinfo["city"].decode("iso-8859-1")
    else:
        city = None
    if geoinfo and geoinfo["region"]:
        region = geoinfo["region"].decode("iso-8859-1")
    else:
        region = None
    if geoinfo and geoinfo["region_name"]:
        region_name = geoinfo["region_name"].decode("iso-8859-1")
    else:
        region_name = None

    asn = lookup_probe_asn(settings.geoip_directory,
                           test_info["client_ip"],
                           test_info["start_timestamp"])

    report_header = {
        "options": {},
        "probe_asn": asn,
        "probe_cc": country_code,
        "probe_ip": test_info["client_ip"],
        "software_name": "Glasnost",
        "version": "v2",
        "start_time": test_info["start_timestamp"],
        "test_name": test_info["proto"],
        "test_version": "v2",
        "data_format_version": "v2",
    }

    dtp = datetime.datetime.utcfromtimestamp(test_info["start_timestamp"])

    yamloo_filename  = "glasnost_%s-" % test_info["proto"]
    yamloo_filename += dtp.strftime("%Y-%m-%dT%H%M%S.%fZ-")
    yamloo_filename += asn
    yamloo_filename += "-"
    yamloo_filename += test_info["mlab_server"]
    yamloo_filename += "-probe.yamloo"

    yamloo_filepath = os.path.join(settings.reports_directory,
                                   yamloo_filename)

    yamloo_file = open(yamloo_filepath, "wb")

    yaml.safe_dump(report_header, yamloo_file, explicit_start=True,
                   explicit_end=True, default_flow_style=False)

    report = {
        "probe_city": city,
        "probe_region": region,
        "probe_region_name": region_name,
        "test_info": test_info,
        "measurements": [],
        "result": result,
        "warnings": warnings,
        "interim": interim
    }
    for measurement in measurements:
        report["measurements"].append(measurement.to_dict())

    yaml.safe_dump(report, yamloo_file, explicit_start=True,
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
