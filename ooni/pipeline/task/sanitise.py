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
import hashlib
import tarfile

from yaml import safe_load_all
from yaml import safe_dump_all, safe_dump
from ooni.pipeline import settings


def archive_report(report_path):

    # zip files
    tar_name = os.path.split(report_path)[-1]
    tar_file = os.path.join(settings.archive_directory, tar_name) + ".gz"

    if os.path.isfile(tar_file):
            print("Archive does already exist, overwriting")

    with tarfile.open(tar_file, "w:gz") as tar:
        tar.add(report_path)


def list_report_files(directory):
    for dirpath, dirname, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".yamloo"):
                yield os.path.join(dirpath, filename)


class processor(object):
    @staticmethod
    def http_template(entry):
        if 'requests' not in entry or not entry['requests']:
            return entry
        for i, _ in enumerate(entry['requests']):
            try:
                del entry['requests'][i]['response']['body']
            except:
                pass
        return entry

    @staticmethod
    def http_requests(entry):
        entry['headers_diff'] = list(entry['headers_diff'])
        return entry

    @staticmethod
    def scapy_template(entry):
        try:
            entry['answered_packets'] = []
        except:
            pass

        try:
            entry['sent_packets'] = []
        except:
            pass
        return entry

    @staticmethod
    def dns_template(entry):
        return entry

    @staticmethod
    def dns_consistency(entry):
        entry['tampering'] = entry['tampering'].items()
        return entry

    @staticmethod
    def captive_portal(entry):
        entry['vendor_dns_tests']['google_dns_cp'] = \
            list(entry['vendor_dns_tests']['google_dns_cp'])
        return entry

    @staticmethod
    def null(entry):
        return entry

    @staticmethod
    def bridge_reachability_tcp_connect(entry):
        if entry['input'].strip() in settings.bridge_db_mapping.keys():
            b = settings.bridge_db_mapping[entry['input'].strip()]
            fingerprint = b['fingerprint'].decode('hex')
            hashed_fingerprint = hashlib.sha1(fingerprint).hexdigest()
            entry['bridge_hashed_fingerprint'] = hashed_fingerprint
            entry['input'] = hashed_fingerprint
            return entry
        return entry

    @staticmethod
    def bridge_reachability(entry):
        if entry.get('bridge_address') and \
                entry['bridge_address'].strip() in settings.bridge_db_mapping:
            b = settings.bridge_db_mapping[entry['bridge_address'].strip()]
            entry['distributor'] = b['distributor']
            fingerprint = b['fingerprint'].decode('hex')
            hashed_fingerprint = hashlib.sha1(fingerprint).hexdigest()
            entry['input'] = hashed_fingerprint
            entry['bridge_address'] = None
        else:
            entry['distributor'] = None
            hashed_fingerprint = None
            pass

        entry['bridge_hashed_fingerprint'] = hashed_fingerprint

        return entry

    @staticmethod
    def tcp_connect(entry):
        entry = processor.bridge_reachability(entry)
        return entry

    @staticmethod
    def default(entry):
        if 'report' in entry:
            entry = entry['report']
        return entry

report_processor = {
    "http_host": processor.http_template,
    "HTTP Host": processor.http_template,

    "http_requests_test": [processor.http_template, processor.http_requests],
    "http_requests": [processor.http_template, processor.http_requests],
    "HTTP Requests Test": [processor.http_template, processor.http_requests],

    "bridge_reachability": processor.bridge_reachability,
    "bridgereachability": processor.bridge_reachability,

    "TCP Connect": processor.tcp_connect,
    "tcp_connect": processor.tcp_connect,

    "DNS tamper": [processor.dns_template, processor.dns_consistency],
    "dns_consistency": [processor.dns_template, processor.dns_consistency],

    "HTTP Invalid Request Line": processor.null,
    "http_invalid_request_line": processor.null,

    "http_header_field_manipulation": processor.null,
    "HTTP Header Field Manipulation": processor.null,

    "Multi Protocol Traceroute Test": [processor.scapy_template],
    "multi_protocol_traceroute_test": [processor.scapy_template],
    "traceroute": [processor.scapy_template],

    "parasitic_traceroute_test": processor.null,

    "tls-handshake": processor.null,

    "dns_injection": processor.null,

    "captivep": processor.captive_portal,
    "captiveportal": processor.captive_portal,

    # These are ignored as we don't yet have analytics for them
    "HTTPFilteringBypass": False,
    "HTTPTrix": False,
    "http_test": False,
    "http_url_list": False,
    "dns_spoof": False,
    "netalyzrwrapper": False,

    # These are ignored because not code for them is available
    "tor_http_requests_test": False,
    "sip_requests_test": False,
    "tor_exit_ip_test": False,
    "website_probe": False,
    "base_tcp_test": False,

    # These are ignored because they are invalid reports
    "summary": False,
    "test_get": False,
    "test_put": False,
    "test_post": False,
    "this_test_is_nameless": False,
    "test_send_host_header": False,
    "test_random_big_request_method": False,
    "test_get_random_capitalization": False,
    "test_put_random_capitalization": False,
    "test_post_random_capitalization": False,
    "test_random_invalid_field_count": False,
    "keyword_filtering_detection_based_on_rst_packets": False
}


class Report(object):
    def __init__(self, path):
        self.fh = open(path)
        self._report = safe_load_all(self.fh)
        self.report_path = path
        self.header = self._report.next()

    def process(self, entry):
        entry = processor.default(entry)
        try:
            ps = report_processor[self.header['test_name']]
        except:
            print("Unknown processor for %s: %s" %
                  (self.header['test_name'], self.report_path))
            ps = False

        if isinstance(ps, list):
            for p in ps:
                try:
                    entry = p(entry)
                except:
                    entry = entry
            return entry
        elif hasattr(ps, '__call__'):
            try:
                return ps(entry)
            except:
                return entry
        elif ps is False:
            return False
        else:
            raise Exception("Invalid definition of processor of the test")

    def next_entry(self):
        try:
            entry = self._report.next()
        except StopIteration:
            raise StopIteration
        except Exception:
            self.next_entry()
        if not entry:
            entry = self.next_entry()
        entry = self.process(entry)
        return entry

    def next(self):
        return self.next_entry()

    def __iter__(self):
        return self

    def close(self):
        self.fh.close()


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

    # iterate over report files
    for report_file in list_report_files(settings.reports_directory):

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
            print("Sanitised report name already exists, overwriting: %s",
                  report_filename_sanitised)
        else:
            print("New report file: %s",
                  report_filename_sanitised)

        report_file_sanitised = open(report_filename_sanitised, 'w')

        safe_dump(report_header, report_file_sanitised, explicit_start=True,
                  explicit_end=True)

        safe_dump_all(report, report_file_sanitised, explicit_start=True,
                      explicit_end=True, default_flow_style=False)

        print("Moving original unsanitised file %s to archive", report_file)

        archive_report(report_file)

        report.close()

        os.remove(report_file)

        report_counter += 1

    if report_counter > 0:
        print("%d reports archived", report_counter)
    else:
        print("No reports were found in the: %s", settings.reports_directory)

if __name__ == "__main__":
    main()
