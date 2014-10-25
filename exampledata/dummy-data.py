import random
from pymongo import MongoClient
import datetime

client = MongoClient('127.0.0.1', 27017)
db = client.ooni

def add_random_to_reports(asn, country, random_date):
    header = {
        "input_hashes": ["9c69e84894aafe3e1925432a2db62fe6713051334f2c357d2cb59323a365c0cb"],
        "options": ["-f", "bridges.txt", "-t", '400'],
        "probe_asn": asn,
        "probe_cc": country,
        "probe_city": None,
        "probe_ip": "127.0.0.1",
        "software_name": "ooniprobe",
        "software_version": "1.2.2",
        "start_time": float(random_date.strftime("%s")),
        "test_name": "bridge_reachability",
        "test_version": "0.1.1"
    }
    return db.reports.insert(header)

def add_randoms_to_measurement(report_id, asn, country, random_date, pt, bridge_hash, dist):
    entry = {
        "bridge_address": None,
        "error": "missing-fteproxy",
        "input": bridge_hash,
        "bridge_hashed_fingerprint": bridge_hash,
        "bridge_fingerprint": None,
        "obfsproxy_version": "0.2.12",
        "success": random.choice([True, False]),
        "timeout": 600,
        "tor_log": None,
        "tor_progress": 100,
        "start_time": float(int(random_date.strftime("%s")) + random.randint(0, 100)),
        "tor_progress_summary": None,
        "tor_progress_tag": None,
        "tor_version": "0.2.5.7-rc",
        "transport_name": pt,
        "report_id": report_id
    }
    db.measurements.insert(entry)

countries = [
    ("ASN4242", "IR"),
    ("ASN1234", "CN"),
    ("ASN4321", "NL"),
]
bridge_hashes = [
    ("obfs3", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "https"),
    ("obfs2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "email"),
    ("ss", "cccccccccccccccccccccccccccccccccccccccc", "tbb"),
    ("fte", "dddddddddddddddddddddddddddddddddddddddd", "private"),
    ("vanilla", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "https"),
    ("obfs4", "ffffffffffffffffffffffffffffffffffffffff", "tbb")
]

latest_time = datetime.datetime.today()
numdays = 100
for asn, country in countries:
    for x in range (0, numdays):
        date = latest_time - datetime.timedelta(days = x)
        random_date = date - datetime.timedelta(minutes = random.randint(0, 420))
        report_id = add_random_to_reports(asn, country, random_date)
        for pt, bridge_hash, dist in bridge_hashes:
            add_randoms_to_measurement(report_id, asn, country, random_date, pt, bridge_hash, dist)
