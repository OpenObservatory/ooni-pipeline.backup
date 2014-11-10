import re
import hashlib
from ooni.pipeline import settings


def http_template(entry):
    return entry


def http_requests(entry):
    return entry


def scapy_template(entry):
    return entry


def dns_template(entry):
    return entry


def dns_consistency(entry):
    return entry


def captive_portal(entry):
    return entry


def null(entry):
    return entry


def bridge_reachability_tcp_connect(entry):
    if entry['input'].strip() in settings.bridge_db_mapping.keys():
        b = settings.bridge_db_mapping[entry['input'].strip()]
        fingerprint = b['fingerprint'].decode('hex')
        hashed_fingerprint = hashlib.sha1(fingerprint).hexdigest()
        entry['bridge_hashed_fingerprint'] = hashed_fingerprint
        entry['input'] = hashed_fingerprint
        return entry
    return entry


def bridge_reachability(entry):
    if entry.get('bridge_address') and \
            entry['bridge_address'].strip() in settings.bridge_db_mapping:
        b = settings.bridge_db_mapping[entry['bridge_address'].strip()]
        entry['distributor'] = b['distributor']
        entry['transport'] = b['transport']
        fingerprint = b['fingerprint'].decode('hex')
        hashed_fingerprint = hashlib.sha1(fingerprint).hexdigest()
        entry['input'] = hashed_fingerprint
        entry['bridge_address'] = None
        regexp = ("(Learned fingerprint ([A-Z0-9]+)"
                  "\s+for bridge (([0-9]+\.){3}[0-9]+\:\d+))|"
                  "((new bridge descriptor .+?\s+"
                  "at (([0-9]+\.){3}[0-9]+)))")
        entry['tor_log'] = re.sub(regexp, "[REDACTED]", entry['tor_log'])
    else:
        entry['distributor'] = None
        hashed_fingerprint = None
        pass

    entry['bridge_hashed_fingerprint'] = hashed_fingerprint

    return entry


def tcp_connect(entry):
    entry = bridge_reachability_tcp_connect(entry)
    return entry


def default(entry):
    return entry
