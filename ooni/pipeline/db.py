import os
from ooni.pipeline import settings
from ooni.pipeline.settings import log


class Database(object):
    def __init__(self):
        self.ip = settings.db_ip
        self.port = settings.db_port
        self.client = None
        self.db = None

    def connect(self):
        raise NotImplemented("connect method must be overriden")


class MongoDatabase(object):
    def connect(self):
        from pymongo import MongoClient
        self.client = MongoClient(self.ip, self.port)
        self.db = self.client.ooni

    def new_report(self, report):
        return self.db.reports.insert(report)

    def add_measurement(self, report_id, measurement):
        self.db.reports.update(
            {'_id': report_id},
            {'$push': {'measurements': measurement}}
        )


class ElasticsearchDatabase(object):
    def connect(self):
        from elasticsearch import Elasticsearch
        # import certifi

        self.es = Elasticsearch(
            [self.ip],
            # http_auth=('user', 'secret'),
            port=self.port,
            # use_ssl=True,
            # verify_certs=True,
            # ca_certs=certifi.where(),
        )

    def new_report(self, report):
        report = self.es.create(
            "reports",
            "report",
            report
        )
        return report["_id"]

    def add_measurement(self, report_id, measurement):
        self.es.update(
            "reports", "report", report_id,
            {
                "script": "ctx._source.measurements += measurement",
                "params": {
                    "measurement": measurement
                }
            }
        )


def create_database():
    if os.environ.get('ELASTICSEARCH'):
        db = ElasticsearchDatabase()
    else:
        db = MongoDatabase()

    try:
        db.connect()
    except Exception as exc:
        log.error("Error in connecting to DB")
        log.error(str(exc))
        raise
