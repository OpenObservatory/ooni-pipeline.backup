# from collections import defaultdict
# from heapq import nlargest

# from luigi import six

import os
import time
import errno
from datetime import datetime

import yaml

import luigi
import luigi.format
import luigi.hadoop
import luigi.hdfs
import luigi.postgres


class ReportsMixin(object):
    required_report_header_keys = ("probe_asn", "software_name",
                                   "software_version", "start_time",
                                   "test_name", "test_version")
    optional_report_header_keys = ("data_format_version", "report_id",
                                   "test_helpers", "options", "probe_ip",
                                   "probe_asn")

    def dump(self, out_file, report_details, report_entries):
        yaml.safe_dump(report_details, out_file, explicit_start=True,
                       explicit_end=True)
        yaml.safe_dump_all(report_entries, out_file, explicit_start=True,
                           explicit_end=True, default_flow_style=False)

    def iterate(self, inputs):
        for input in inputs:
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


class SanitisedArchive(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path, format=luigi.format.GzipFormat())


class GenerateStreams(luigi.Task, ReportsMixin):
    reports_dir = '/data1/pipeline/sanitised-archive'
    streams_dir = '/data1/reports/streams/'

    def requires(self):
        for dirname, dirnames, filenames in os.walk(self.reports_dir):
            for filename in filenames:
                yield SanitisedArchive(os.path.join(dirname, filename))

    def output(self):
        return luigi.LocalTarget(self.stream_filename)

    def run(self):
        for report_details, report_entries in self.iterate(self.input()):
            st = report_details['start_time']
            self.stream_filename = os.path.join(
                self.streams_dir,
                datetime.fromtimestamp(st).strftime("%Y-%m-%d")
            )
            with self.output().open('a') as out_file:
                self.dump(out_file, report_details, report_entries)


class ReportStreams(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        filename = self.date.strftime('/reports/streams/%Y-%m-%d')
        return luigi.hdfs.HdfsTarget(filename)
        # hdfs_client = luigi.hdfs.HdfsClient()
        # for file in hdfs_client.listdir("reports/"):
        #     yield luigi.hdfs.HdfsTarget(file)


class AggregateReports(luigi.Task, ReportsMixin):
    # date = luigi.DateParameter()
    # report_filename = luigi.Parameter()
    # country_code = luigi.Parameter()
    date_range = luigi.DateIntervalParameter()
    table_name = "reports"

    def output(self, report_details):
        report_file_template = "/reports/public/{probe_cc}/" \
                               "{iso8601_timestamp}-{test_name}-" \
                               "{probe_cc}-{probe_asn}"
        report_path = report_file_template.format(**report_details)
        return luigi.hdfs.HdfsTarget(report_path)

    def requires(self):
        return [ReportStreams(date) for date in self.date_interval]

    def connect_to_dynamodb(self, region='us-east-1', aws_access_key_id=None,
                            aws_secret_access_key=None):
        import boto.dynamodb2
        config = luigi.configuration.get_config()
        if not aws_access_key_id:
            aws_access_key_id = config.get('dynamodb', 'aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = config.get_config().get(
                'dynamodb', 'aws_secret_access_key'
            )
        self.dynamo_cx = boto.dynamodb2.connect_to_region(
            region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            is_secure=True
        )
        self.exists()

    def exists(self):
        from boto.dynamodb2.table import Table
        from boto.dynamodb2.fields import HashKey
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
        for report_details, report_entries in self.iterate(self.input()):
            with self.output(report_details).open('w') as out_file:
                self.dump(out_file, report_details, report_entries)
                self.add_report_to_db(report_details, report_entries)

if __name__ == "__main__":
    luigi.run()
