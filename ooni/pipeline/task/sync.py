#!/usr/bin/env python

# 1 connect to remote server via ssh. read in all the yaml files
# 2 check if same yaml is in santisied or raw
# 3 if file is missing copy it to raw

from __future__ import print_function

import re
import os
import sys
import yaml
import shutil
import tempfile

from ooni.pipeline import settings


def list_report_files(directory):
    for dirpath, dirname, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".yamloo"):
                yield os.path.join(dirpath, filename)


class Report(object):
    def __init__(self, path):
        self.fh = open(path)
        self._report = yaml.safe_load_all(self.fh)
        self.report_path = path
        self.header = self._report.next()

        self.asn = self.header['probe_asn']
        self.start_time = self.header['start_time']
        self.test_name = self.header['test_name']
        self.input_hashes = self.header['input_hashes']

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.asn != other.asn:
                return False
            if self.start_time != other.start_time:
                return False
            if self.test_name != other.test_name:
                return False
            if self.input_hashes != other.input_hashes:
                return False

            return True
        else:
            return True

    def __ne__(self, other):
            return not self.__eq__(other)

    def dump_header(self):
        print("%s %s %s %s %s" % (self.asn, self.start_time, self.test_name,
                                  self.input_hashes))

    def close(self):
        self.fh.close()


def check_if_report_in_database(report):
    result = settings.db.reports.find_one({
        "probe_asn": report.asn,
        "start_time": report.start_time,
        "test_name": report.test_name,
        "input_hashes": report.input_hashes
    })

    if result:
        return True
    return False


def get_report_list_via_rsync(remote):
    """
    checks if new reports are available remotely and returns copies
    them to a temporary if this is the case.
    the name of the directory is returned by this method
    """
    rsync_cmd = "rsync --remove-source-files -avz -e ssh "

    temp_dir = tempfile.mkdtemp()

    print("\nChecking for reports remotely on %s" % remote)

    # FIXME this is vulnerable to command injection
    if os.system(rsync_cmd + remote + " " + temp_dir):
        print("An error occured while checking the remote server: %s" % remote)

    if os.listdir(temp_dir) == []:
        print("No yaml files found on remote server")
        shutil.rmtree(temp_dir)
        return None

    print("Some yaml files were found on the server "
          "and copied to %s" % temp_dir)
    return temp_dir


def read_reports_from_dir(dir):
    reports = []
    for report_file in list_report_files(dir):
            match = re.search("^" + re.escape(dir) + "(.*)",
                              report_file)

            # read report file
            report = Report(report_file)
            e = report.header
            e['report_file'] = match.group(1)

            reports.append(report)
            report.close()

    return reports


def readin_local_reports(directories):
    """
    read in all local reports from different directories
    """
    print("Reading in local reports from: %s" % directories)
    reports = []

    for dir in directories:
        # merge arrays
        reports += read_reports_from_dir(dir)

    return reports


def process(remote_servers):
    local_reports = readin_local_reports([settings.raw_directory,
                                          settings.sanitised_directory])
    count_reports = 0

    for server in remote_servers:

        # copies reports from remote server to temporary directory
        temp_dir = get_report_list_via_rsync(server)

        if temp_dir is not None:
            remote_reports = read_reports_from_dir(temp_dir)
        else:
            break

        for report in remote_reports:

            if report in local_reports:
                # report is in raw or santised directories
                pass
            elif check_if_report_in_database(report):
                # report was found in database
                pass
            else:
                print("Copying report into into raw directory: %s %s" %
                      (report.report_path, settings.raw_directory))

                report_file = os.path.split(report.report_path)[-1]
                newname = os.path.join(settings.raw_directory, report_file)
                os.rename(report.report_path, newname)
                count_reports += 1

        print("Found %d reports in %s that "
              "were not locally present and copied them to: %s" %
              (count_reports, server, settings.raw_directory))

        shutil.rmtree(temp_dir)


def main():
    if not os.path.isdir(settings.raw_directory):
        print("%s does not exist" % settings.raw_directory)
        sys.exit(1)

    if not os.path.isdir(settings.sanitised_directory):
        print("%s does not exist" % settings.sanitised_directory)
        sys.exit(1)

    if not os.path.isfile(settings.remote_servers_file):
        print("%s does not exist" % settings.remote_servers_file)
        sys.exit(1)

    with open(settings.remote_servers_file) as f:
            remote_servers = [line.strip() for line in f]

    process(remote_servers)

if __name__ == "__main__":
    main()
