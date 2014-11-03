from yaml import safe_load_all

from ooni.pipeline import processor


class Report(object):
    name_to_processor = {
        "http_host": processor.http_template,
        "HTTP Host": processor.http_template,

        "http_requests_test": [processor.http_template,
                               processor.http_requests],
        "http_requests": [processor.http_template, processor.http_requests],
        "HTTP Requests Test": [processor.http_template,
                               processor.http_requests],

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

    def __init__(self, path):
        self.fh = open(path)
        self._report = safe_load_all(self.fh)
        self.report_path = path
        self.header = self._report.next()

    def process(self, entry):
        entry = processor.default(entry)
        try:
            ps = self.name_to_processor[self.header['test_name']]
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
