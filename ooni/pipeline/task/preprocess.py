""" Preprocess M-Lab files converting them in .yamloo files """

# TODO: adapt this code to process all M-Lab files rather than only Glasnost

import StringIO
import os
import re
import tarfile

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

    measurements, _ = glasnost.parse_summary_string_log2(
        test_info["client_sum"], test_info["server_sum"])

    print measurements

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
