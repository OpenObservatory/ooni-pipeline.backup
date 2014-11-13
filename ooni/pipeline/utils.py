from datetime import datetime, timedelta, tzinfo


class UTC(tzinfo):
    """UTC"""
    ZERO = timedelta(0)

    def utcoffset(self, dt):
        return self.ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return self.ZERO


def convert2unicode(dictionary):
    for k, v in dictionary.iteritems():
        if isinstance(v, str):
            dictionary[k] = unicode(v, errors='replace')
        elif isinstance(v, dict):
            convert2unicode(v)


def epoch_to_timestamp(seconds_since_epoch):
    date = datetime.fromtimestamp(seconds_since_epoch, UTC())
    ISO8601 = "%Y-%m-%dT%H%M%SZ"
    return date.strftime(ISO8601)


def generate_filename(report_header):
    timestamp = epoch_to_timestamp(report_header['start_time'])
    return "{test_name}-{timestamp}-{asn}-probe.yamloo".format(
        test_name=report_header['test_name'],
        timestamp=timestamp,
        asn=report_header['probe_asn']
    )
