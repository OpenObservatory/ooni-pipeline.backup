from ooni.processor import process, sanitise

sanitisers = {
    "http_host": sanitise.http_template,
    "HTTP Host": sanitise.http_template,

    "http_requests_test": [sanitise.http_template,
                           sanitise.http_requests],
    "http_requests": [sanitise.http_template, sanitise.http_requests],
    "HTTP Requests Test": [sanitise.http_template,
                           sanitise.http_requests],

    "bridge_reachability": sanitise.bridge_reachability,
    "bridgereachability": sanitise.bridge_reachability,

    "TCP Connect": sanitise.tcp_connect,
    "tcp_connect": sanitise.tcp_connect,

    "DNS tamper": [sanitise.dns_template, sanitise.dns_consistency],
    "dns_consistency": [sanitise.dns_template, sanitise.dns_consistency],

    "HTTP Invalid Request Line": sanitise.null,
    "http_invalid_request_line": sanitise.null,

    "http_header_field_manipulation": sanitise.null,
    "HTTP Header Field Manipulation": sanitise.null,

    "Multi Protocol Traceroute Test": [sanitise.scapy_template],
    "multi_protocol_traceroute_test": [sanitise.scapy_template],
    "traceroute": [sanitise.scapy_template],

    "parasitic_traceroute_test": sanitise.null,

    "tls-handshake": sanitise.null,

    "dns_injection": sanitise.null,

    "captivep": sanitise.captive_portal,
    "captiveportal": sanitise.captive_portal,

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
    "keyword_filtering_detection_based_on_rst_packets": False,
    "default": sanitise.default
}

processors = {
    "http_host": process.http_template,
    "HTTP Host": process.http_template,

    "http_requests_test": [process.http_template,
                           process.http_requests],
    "http_requests": [process.http_template, process.http_requests],
    "HTTP Requests Test": [process.http_template,
                           process.http_requests],

    "bridge_reachability": process.bridge_reachability,
    "bridgereachability": process.bridge_reachability,

    "TCP Connect": process.tcp_connect,
    "tcp_connect": process.tcp_connect,

    "DNS tamper": [process.dns_template, process.dns_consistency],
    "dns_consistency": [process.dns_template, process.dns_consistency],

    "HTTP Invalid Request Line": process.null,
    "http_invalid_request_line": process.null,

    "http_header_field_manipulation": process.null,
    "HTTP Header Field Manipulation": process.null,

    "Multi Protocol Traceroute Test": [process.scapy_template],
    "multi_protocol_traceroute_test": [process.scapy_template],
    "traceroute": [process.scapy_template],

    "parasitic_traceroute_test": process.null,

    "tls-handshake": process.null,

    "dns_injection": process.null,

    "captivep": process.captive_portal,
    "captiveportal": process.captive_portal,

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
    "keyword_filtering_detection_based_on_rst_packets": False,
    "default": process.default
}


def run(test_name, report_path, entry, mapping):
    entry = mapping["default"](entry)
    try:
        ps = mapping[test_name]
    except:
        print("Unknown processor for %s: %s" %
              (test_name, report_path))
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


def run_process(test_name, report_path, entry):
    return run(test_name, report_path, entry, processors)


def run_sanitise(test_name, report_path, entry):
    return run(test_name, report_path, entry, sanitisers)
