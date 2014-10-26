import sys
import os

from ooni.pipeline import task
from ooni.pipeline import settings

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
        task.export.main(settings.bridge_db_filename, settings.bridge_by_country_code_output)
    elif task_name == "sanitise":
        task.sanitise.main()
    elif task_name == "import":
        task.publish.main()
    elif task_name == "sync":
        task.sync.main()
    else:
        print("Invalid command!")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    task_name = sys.argv[1]
    run(task_name)
