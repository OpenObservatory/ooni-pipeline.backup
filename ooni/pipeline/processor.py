import hashlib
from ooni.pipeline import settings


def http_template(entry):
    if 'requests' not in entry or not entry['requests']:
        return entry
    for i, _ in enumerate(entry['requests']):
        try:
            del entry['requests'][i]['response']['body']
        except:
            pass
    return entry


def http_requests(entry):
    entry['headers_diff'] = list(entry['headers_diff'])
    return entry


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


def dns_template(entry):
    return entry


def dns_consistency(entry):
    entry['tampering'] = entry['tampering'].items()
    return entry


def captive_portal(entry):
    entry['vendor_dns_tests']['google_dns_cp'] = \
        list(entry['vendor_dns_tests']['google_dns_cp'])
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


def tcp_connect(entry):
    entry = bridge_reachability(entry)
    return entry


def default(entry):
    if 'report' in entry:
        entry = entry['report']
    return entry
