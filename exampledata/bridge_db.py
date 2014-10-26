import json
import hashlib
bridge_db = {
  'IP:PORT': {'distributor': 'tbb',
                           'fingerprint': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                           'transport': 'vanilla'},
  'IP:PORT': {'distributor': 'tbb',
                           'fingerprint': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                           'transport': 'vanilla'},
  'IP:PORT': {'distributor': 'tbb',
                           'fingerprint': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                           'transport': 'vanilla'},
  'IP:PORT': {'distributor': 'tbb',
                           'fingerprint': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                           'transport': 'vanilla'}
}
with open("bridge_db.json", "w") as f:
  for k,v in bridge_db.items():
    hashed_fingerprint = hashlib.sha1(v['fingerprint'].decode('hex'))
    hashed_fingerprint = hashed_fingerprint.hexdigest()
    bridge_db[k]['hashed_fingerprint'] = hashed_fingerprint
  f.write(json.dumps(bridge_db))
