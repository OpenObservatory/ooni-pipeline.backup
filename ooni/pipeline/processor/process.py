from ooni.pipeline.utils import convert2unicode


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
    return entry


def bridge_reachability(entry):
    return entry


def tcp_connect(entry):
    return entry


def default(entry):
    if 'report' in entry:
        entry = entry['report']
    convert2unicode(entry)
    return entry
