# import os
# from collections import defaultdict
# from heapq import nlargest

# from luigi import six

import yaml

import luigi
import luigi.hadoop
import luigi.hdfs
import luigi.postgres

import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey

class ReportStreams(luigi.ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.date.strftime('/reports/streams/%Y-%m-%d'))
        # hdfs_client = luigi.hdfs.HdfsClient()
        # for file in hdfs_client.listdir("reports/"):
        #     yield luigi.hdfs.HdfsTarget(file)

class AggregateReports(luigi.Task):
    # date = luigi.DateParameter()
    # report_filename = luigi.Parameter()
    # country_code = luigi.Parameter()
    date_range = luigi.DateIntervalParameter()
    required_report_header_keys = ("probe_asn", "software_name", "software_version",
                                    "start_time", "test_name", "test_version")
    optional_report_header_keys = ("data_format_version", "report_id",
                                    "test_helpers", "options", "probe_ip", "probe_asn")
    table_name = "reports"

    def output(self, report_details):
        report_file_template = "/reports/public/{probe_cc}/{iso8601_timestamp}-{test_name}-{probe_cc}-{probe_asn}"
        report_path = report_file_template.format(**report_details)
        return luigi.hdfs.HdfsTarget(report_path)

    def requires(self):
        return [ReportStreams(date) for date in self.date_interval]

    def _input_iterator(self):
        for input in self.input():
            with input.open('r') as in_file:
                report = yaml.safe_load_all(in_file)

                report_entries = []
                report_details = report.next()
                for report_entry in report:
                    if all(key in report_entry.keys()
                           for key in self.required_report_header_keys):
                        yield report_details, report_entries
                        report_details = report_entry
                        report_entries = []
                    else:
                        report_entries.append(report_entry)

    def connect_to_dynamodb(self, region='us-east-1', aws_access_key_id=None, aws_secret_access_key=None):
        if not aws_access_key_id:
            aws_access_key_id = luigi.configuration.get_config().get('dynamodb', 'aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = luigi.configuration.get_config().get('dynamodb', 'aws_secret_access_key')
        self.dynamo_cx = boto.dynamodb2.connect_to_region(
             region,
             aws_access_key_id=aws_access_key_id,
             aws_secret_access_key=aws_secret_access_key,
             is_secure=True)
        self.exists()

    def exists(self):
        schema = [HashKey(key) for key in self.required_report_header_keys]
        try:
            table = Table.create(
                self.table_name,
                schema=schema,
                connection=self.dynamo_cx
            )
            self.table = table
        except Exception as exc:
            print "XXX need to filter this out %s" % exc
            self.table = Table(self.table_name)

    def connect_to_db(self):
        self.connect_to_dynamodb()

    def add_to_dynamodb(self, report_details):
        self.table.put_item(data=report_details)

    def add_report_to_db(self, report_details, report_entries):
        report_details["measurements"] = report_entries
        self.add_to_dynamo_db(report_details)

    def run(self):
        self.connect_to_db()
        for report_details, report_entries in self._input_iterator():
            with self.output(report_details).open('w') as out_file:
                yaml.safe_dump(report_details, out_file, explicit_start=True,
                               explicit_end=True)
                yaml.safe_dump_all(report_entries, out_file, explicit_start=True,
                                   explicit_end=True, default_flow_style=False)
                self.add_report_to_db(report_details, report_entries)
