from . import glasnost

import GeoIP
import datetime
import os

ASN_DATABASES = {}
CITY_DATABASES = {}

#
# TODO: We can do better than we currently do with respect to
# country and AS number information.
#
# What we currently do is that we expect users of this module to
# have a folder containing GeoLiteCity and GeoIPASNum databases
# for each year and month of the analysis.
#
# In reality, I myself only have some of these databases, and I
# usually pick the closest database when the exact database needed
# is not available. This is done by copying the closest database
# in the proper folder in which it is missing.
#
# Possible ways to improve all of this:
#
# 0) Use GeoLite when such information is available for that month.
#
# 1) Use code from the original Glasnost parser to pick automatically
#    the closest available database.
#
# 2) For AS number information rely on PyASN (by Hadi Asghari, the
#    author of the Glasnost parser) that also provides historical
#    information on the AS numbers allocation in the past.
#
#        Ref: <https://github.com/hadiasghari/pyasn>
#
# 3) For country information we could use the database maintained
#    at <http://software77.net/geo-ip/> that also provides historical
#    information going back to 2011.
#

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
