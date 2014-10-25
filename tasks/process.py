#!/usr/bin/env python
# To be run with python process.py /data/collector/archive

from __future__ import print_function

import re
import os
import sys
import json
import hashlib
import yaml

from pymongo import MongoClient

if len(sys.argv) != 2:
    print("Usage: %s <reports_directory>" % sys.argv[0])
    sys.exit(1)

bridge_db_mapping_file = "path_to_ip_port_based_mapping.json"
reports_directory = sys.argv[1]
bridge_db_mapping = json.load(open(bridge_db_mapping_file))

class processor(object):
    @staticmethod
    def http_template(entry):
        if 'requests' not in entry or not entry['requests']:
          return entry
        for i, _ in enumerate(entry['requests']):
            try: del entry['requests'][i]['response']['body']
            except: pass
        return entry

    @staticmethod
    def http_requests(entry):
        try: entry['headers_diff'] = list(entry['headers_diff'])
        except: pass
        return entry

    @staticmethod
    def scapy_template(entry):
        try: entry['answered_packets'] = []
        except: pass

        try: entry['sent_packets'] = []
        except: pass
        return entry

    @staticmethod
    def dns_template(entry):
        return entry

    @staticmethod
    def dns_consistency(entry):
        try: entry['tampering'] = entry['tampering'].items()
        except: pass
        return entry

    @staticmethod
    def captive_portal(entry):
        try: entry['vendor_dns_tests']['google_dns_cp'] = list(entry['vendor_dns_tests']['google_dns_cp'])
        except: pass
        return entry

    @staticmethod
    def null(entry):
        return entry

    @staticmethod
    def bridge_reachability_tcp_connect(entry):
        if entry['input'].strip() in bridge_db_mapping.keys():
            b = bridge_db_mapping[entry['input'].strip()]
            fingerprint = b['fingerprint']
            hashed_fingerprint = hashlib.sha1(fingerprint.decode('hex')).hexdigest()
            entry['bridge_hashed_fingerprint'] = hashed_fingerprint
            entry['input'] = hashed_fingerprint
            return entry
        return entry

    @staticmethod
    def bridge_reachability(entry):
        b = bridge_db_mapping[entry['bridge_address'].strip()]
        fingerprint = b['fingerprint']
        hashed_fingerprint = hashlib.sha1(fingerprint.decode('hex')).hexdigest()
        entry['bridge_hashed_fingerprint'] = hashed_fingerprint
        entry['bridge_address'] = None
        entry['input'] = hashed_fingerprint
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

    def process(self, entry):
        entry = processor.default(entry)
        try:
            ps = report_processor[self.header['test_name']]
        except:
             print("Unknown processor for %s: %s" %
                   (self.header['test_name'], self.report_path))

        if isinstance(ps, list):
            for p in ps:
                entry = p(entry)
            return entry
        elif hasattr(ps, '__call__'):
            return ps(entry)
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

client = MongoClient('172.17.1.174', 27017)
db = client.bridge_reachability

report_list = []
for report_file in list_report_files(reports_directory):
    match = re.search("^" + re.escape(reports_directory) + "(.*)",
                      report_file)
    report = Report(report_file)
    e = report.header
    e['report_file'] = match.group(1)
    r = db.reports.find_one({
      'test_name': e['test_name'],
      'start_time': e['start_time'],
      'probe_cc': e['probe_cc']
    })
    if r:
      print("%s already present" % match.group(1))
      report_id = r['_id']
    else:
      print("Adding %s" % match.group(1))
      report_id = db.reports.insert(e)
    for entry in report:
        if not entry:
            print("Ignoring")
            break
        entry['report_id'] = report_id
        try:
            if not db.measurements.find_one(entry):
                sys.stdout.write(".")
                db.measurements.insert(entry)
            else:
                print("ALREADY THERE")
                print(entry)
                sys.stdout.write("o")
        except Exception as exception:
            print(exception)
            sys.stdout.write("X")
        sys.stdout.flush()
    print("\n------")
    report.close()
