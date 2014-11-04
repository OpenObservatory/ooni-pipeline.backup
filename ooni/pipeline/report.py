from yaml import safe_load_all

from ooni.pipeline.processor import run_process, run_sanitise


class Report(object):

    def __init__(self, path, action="sanitise"):
        self.fh = open(path)
        self._report = safe_load_all(self.fh)
        self.report_path = path
        self.header = self._report.next()
        self.action = action

    def sanitise(self, entry):
        return run_sanitise(self.header['test_name'], self.report_path, entry)

    def process(self, entry):
        return run_process(self.header['test_name'], self.report_path, entry)

    def next_entry(self):
        try:
            entry = self._report.next()
        except StopIteration:
            raise StopIteration
        except Exception:
            self.next_entry()
        if not entry:
            entry = self.next_entry()
        if self.action == "sanitise":
            entry = self.sanitise(entry)
        elif self.action == "process":
            entry = self.process(entry)
        return entry

    def next(self):
        return self.next_entry()

    def __iter__(self):
        return self

    def close(self):
        self.fh.close()
