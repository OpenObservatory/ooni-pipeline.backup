""" Preprocess M-Lab files converting them in .yamloo files """

# TODO: adapt this code to process all M-Lab files rather than only Glasnost

import os
import re
import tarfile

from ooni.pipeline import settings

MLAB_FILE_PATTERN = \
"^[0-9]{8}T[0]{6}Z-mlab[1-9]{1}-[a-z]{3}[0-9]{2}-[a-z]+-[0-9]{4}.tgz$"


def list_report_files(directory):
    """ Lists the report files to process """
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if re.match(MLAB_FILE_PATTERN, filename):
                yield os.path.join(dirpath, filename)


def process_glasnost_log(pseudofile):
    """ Process the log of a Glasnost test """
    print pseudofile.read()


def process_glasnost_tarball(filepath):
    """ Process a single glasnost tarball """
    tarball = tarfile.open(filepath, "r")
    for member in tarball:
        if member.isreg():
            if member.path.endswith(".log"):
                pseudofile = tarball.extractfile(member)
                process_glasnost_log(pseudofile)


def main():
    """ Main function """
    for filepath in list_report_files(settings.raw_directory):
        process_glasnost_tarball(filepath)
