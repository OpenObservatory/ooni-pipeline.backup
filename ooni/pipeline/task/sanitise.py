#!/usr/bin/env python
# https://trac.torproject.org/projects/tor/ticket/13563

#
# 1. read yaml files from reports directory
# 2. santised yaml filenames
#
#    for every bridge that is listed in the file <bridge_db_mapping_file> do
#    the following sanitisation
#        - replace input field with a sha1 hash of it and call it
#        bridge_hashed_fingerprint
#        - set bridge_address to "null"
#        - add field called "distributor" which contains the distribution
#        method
#
# 3. archive raw report to archive
#
# 4. remove original report file from <reports_directory> and write sanitised
# file to <santised_directory>

from __future__ import print_function

import re
import os
import sys
import gzip
from multiprocessing import Manager, cpu_count, Pool

from yaml import safe_dump_all, safe_dump
from ooni.pipeline import settings
from ooni.pipeline.report import Report


def archive_report(report_path):

    # zip files
    gz_name = os.path.split(report_path)[-1]
    gz_file = os.path.join(settings.archive_directory, gz_name) + ".gz"

    if os.path.isfile(gz_file):
            print("Archive does already exist, overwriting")

    print("Archiving report: " + report_path)
    infile = open(report_path, 'rb')
    outfile = gzip.open(gz_file, 'wb')
    outfile.writelines(infile)

    infile.close()
    outfile.close()

def list_report_files(directory):
    for dirpath, dirname, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".yamloo"):
                yield os.path.join(dirpath, filename)


def sanitise_report(report_file, semaphore):
    match = re.search("^" + re.escape(settings.reports_directory) + "(.*)",
                        report_file)

    # read report file
    report = Report(report_file)
    report_header = report.header
    report_header['report_file'] = match.group(1)

    report_filename = os.path.split(report_file)[-1]
    report_filename_sanitised = os.path.join(settings.sanitised_directory,
                                                report_filename)

    if os.path.isfile(report_filename_sanitised):
        print("Sanitised report name already exists, overwriting: %s" %
                report_filename_sanitised)
    else:
        print("New report file: %s" %
                report_filename_sanitised)

    report_file_sanitised = open(report_filename_sanitised, 'w')

    safe_dump(report_header, report_file_sanitised, explicit_start=True,
                explicit_end=True)

    safe_dump_all(report, report_file_sanitised, explicit_start=True,
                    explicit_end=True, default_flow_style=False)

    print("Moving original unsanitised file %s to archive" % report_file)

    archive_report(report_file)

    report_file_sanitised.close()
    report.close()

    os.remove(report_file)

    semaphore.release()

def main():
    if not os.path.isdir(settings.archive_directory):
        print(settings.archive_directory + " does not exist")
        sys.exit(1)

    if not os.path.isdir(settings.reports_directory):
        print(settings.reports_directory + " does not exist")
        sys.exit(1)

    if not os.path.isfile(settings.bridge_db_mapping_file):
        print(settings.bridge_db_mapping_file + " does not exist")
        sys.exit(1)

    if not os.path.isdir(settings.sanitised_directory):
        print(settings.sanitised_directory + " does not exist")
        sys.exit(1)

    report_counter = 0

    manager = Manager()
    semaphore = manager.Semaphore(cpu_count())
    pool = Pool(processes=cpu_count())

    # iterate over report files
    report_files = list_report_files(settings.reports_directory)
    while True:
        try:
            semaphore.acquire()
            report_file = report_files.next()
            pool.apply_async(sanitise_report, (report_file, semaphore))
            report_counter += 1

        except StopIteration:
            break

    print("Waiting for all the tasks to finish")
    pool.close()
    pool.join()
    if report_counter > 0:
        print(str(report_counter) + " reports archived")
    else:
        print("No reports were found in the reports directory: "+ settings.reports_directory)

if __name__ == "__main__":
    main()
