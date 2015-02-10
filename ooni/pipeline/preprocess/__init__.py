from . import glasnost

import GeoIP
import datetime
import os

ASN_DATABASES = {}
CITY_DATABASES = {}

def lookup_probe_asn(basedir, probe_ip, start_time):
    """ Lookup probe ASN using GeoIP """
    dtp = datetime.datetime.utcfromtimestamp(start_time)
    lookup = "%04d/%02d" % (dtp.year, dtp.month)
    if lookup not in ASN_DATABASES:
        filepath = os.path.join(basedir, lookup, "GeoIPASNum.dat")
        handle = GeoIP.open(filepath, GeoIP.GEOIP_STANDARD)
        ASN_DATABASES[lookup] = handle
    handle = ASN_DATABASES[lookup]
    org = handle.org_by_addr(probe_ip)
    if not org:
        org = "AS0"
    return org.decode("iso-8859-1").split()[0]

def lookup_geoip(basedir, probe_ip, start_time):
    """ Lookup geoip information """
    dtp = datetime.datetime.utcfromtimestamp(start_time)
    lookup = "%04d/%02d" % (dtp.year, dtp.month)
    if lookup not in CITY_DATABASES:
        filepath = os.path.join(basedir, lookup, "GeoLiteCity.dat")
        handle = GeoIP.open(filepath, GeoIP.GEOIP_STANDARD)
        CITY_DATABASES[lookup] = handle
    handle = CITY_DATABASES[lookup]
    return handle.record_by_addr(probe_ip)
