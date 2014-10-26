import os
import json
from pymongo import MongoClient

raw_directory = os.environ['OONI_RAW_DIR']
sanitised_directory = os.environ['OONI_SANITISED_DIR']
public_directory = os.environ['OONI_PUBLIC_DIR']
reports_directory = os.environ['OONI_RAW_DIR']
archive_directory = os.environ['OONI_ARCHIVE_DIR']

bridge_db_mapping_file = os.environ['OONI_BRIDGE_DB_FILE']
bridge_db_mapping = json.load(open(bridge_db_mapping_file))

bridge_db_filename = os.environ['OONI_BRIDGE_DB_FILE']
bridge_by_country_code_output = os.path.join(os.environ['OONI_PUBLIC_DIR'],
                                             'bridges-by-country-code.json')
db_ip, db_port = os.environ['OONI_DB_IP'], int(os.environ['OONI_DB_PORT'])

mongo_client = MongoClient(db_ip, db_port)
db = mongo_client.ooni
