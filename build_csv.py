import csv
import json
import hashlib
from pymongo import MongoClient

bridge_db_file = '/path_to_hashed_fingerprint_mapping.json'
bridge_db = json.load(open(bridge_db_file))
client = MongoClient('172.17.1.174', 27017)
db = client.bridge_reachability

csvfile = open('bridge_reachability.csv', 'wb')
writer = csv.writer(csvfile)
writer.writerow(["timestamp","country_code","test_name","transport","distributor","fingerprint","success","error"])
reports = db.reports.find({"$or": [{"test_name": "bridge_reachability"},
                                   {"test_name": "tcp_connect"}]})
for report in reports:
    measurements = db.measurements.find({'report_id': report['_id']})

    for measurement in measurements:
      row = [report['start_time'], report['probe_cc'], report['test_name']]
      if report['test_name'] == 'bridge_reachability':
          bridge_hashed_fingerprint = measurement['bridge_hashed_fingerprint']
          success = measurement['success']
          error = measurement['error']
      elif report['test_name'] == 'tcp_connect' and 'connection' in measurement:
          bridge_hashed_fingerprint = measurement['bridge_hashed_fingerprint']
          if measurement['connection'] == "success":
              success = True
              error = None
          else:
              success = False
              error = measurement['connection']
      else:
          continue
      if bridge_hashed_fingerprint:
          bridge_hashed_fingerprint = bridge_hashed_fingerprint.strip()
      if not bridge_hashed_fingerprint:
          continue
      try:
          bridge = bridge_db[bridge_hashed_fingerprint.strip()]
      except:
          print "FAILED TO FIND %s" % bridge_hashed_fingerprint
          continue
      fingerprint = bridge['fingerprint']
      fingerprint = hashlib.sha1(fingerprint.decode('hex')).hexdigest()

      db.measurements
      # row.append(bridge['transport'])
      # row.append(bridge['distributor'])
      # row.append(fingerprint)
      # row.append(success)
      # row.append(error)
      # writer.writerow(row)
