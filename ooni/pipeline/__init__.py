import sys
import os

from ooni.pipeline import task

db_ip, db_port = os.environ['OONI_DB_IP'], int(os.environ['OONI_DB_PORT'])
bridge_db_filename = os.environ['OONI_BRIDGE_DB_FILE']
bridge_by_country_output = os.path.join(os.environ['OONI_PUBLIC_DIR'],
                                        'bridges-by-country-code.json')
sanitised_directory = os.environ['OONI_SANITISED_DIR']
remote_servers_file = os.environ['OONI_REMOTE_SERVERS_FILE']
raw_directory = os.environ['OONI_RAW_DIR']


def usage():
    print("""
Usage: %s <task name>
Task names:
    * export
    * sanitise
    * import
    * sync
""" % sys.argv[0])
    sys.exit(1)


def run(task_name):
    if task_name == "export":
        task.export.main(bridge_db_filename, bridge_by_country_output)
    elif task_name == "sanitise":
        task.sanitise.main()
    elif task_name == "import":
        task.publish.main()
    elif task_name == "sync":
        task.sync.main(raw_directory, sanitised_directory, remote_servers_file,
                       db_ip, db_port)
    else:
        print("Invalid command!")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    task_name = sys.argv[1]
    run(task_name)
