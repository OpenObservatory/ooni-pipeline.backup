import os
import json
from pymongo import MongoClient

raw_directory = os.environ.get('OONI_RAW_DIR')
sanitised_directory = os.environ.get('OONI_SANITISED_DIR')
public_directory = os.environ.get('OONI_PUBLIC_DIR')
reports_directory = os.environ.get('OONI_RAW_DIR')
archive_directory = os.environ.get('OONI_ARCHIVE_DIR')

bridge_db_mapping_file = os.environ.get('OONI_BRIDGE_DB_FILE')
try:
    bridge_db_mapping = json.load(open(bridge_db_mapping_file))
except:
    bridge_db_mapping = None

bridge_db_filename = os.environ.get('OONI_BRIDGE_DB_FILE')
try:
    bridge_by_country_code_output = os.path.join(public_directory,
                                                 'bridges-by-country-code.json')
except:
    bridge_by_country_code_output = None

try:
    db_ip, db_port = os.environ.get('OONI_DB_IP'), int(os.environ.get('OONI_DB_PORT'))

    mongo_client = MongoClient(db_ip, db_port)
    db = mongo_client.ooni
except:
    db_ip, db_port = None, None
    mongo_client = None
    db = None

# maximum distance of control measurement in hours in order to be considered a control
# measurement
# https://trac.torproject.org/projects/tor/ticket/13640
try:
    max_distance_control_measurement = int(os.environ.get('OONI_MAX_DISTANCE_CM'))
except:
    max_distance_control_measurement = None
