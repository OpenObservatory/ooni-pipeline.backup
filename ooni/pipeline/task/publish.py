#!/usr/bin/env python
# This is what used to be called import.py
from os.path import join, basename, dirname
from os import walk, remove, makedirs

import re
import yaml
import gzip
import shutil
import logging
import sys

from ooni.pipeline import settings
from ooni.pipeline.report import WHITELIST
from ooni.pipeline.settings import log
from ooni.pipeline.processor import run_process

from multiprocessing import Manager, cpu_count, Pool


def list_report_files(directory):
    for dirpath, dirname, filenames in walk(directory):
        for filename in filenames:
            if filename.endswith(".yamloo"):
                yield join(dirpath, filename)


class ReportInserter(object):
    def __init__(self, report_file, semaphore):
        try:
            # Insert the report into the database
            self.fh = open(report_file)
            self._report = yaml.safe_load_all(self.fh)
            self.header = self._report.next()
            cc = self.header['probe_cc']
            assert re.match("[a-zA-Z]{2}", cc)

            public_file = join(settings.public_directory, cc,
                               basename(report_file)+".gz")
            self.header['report_file'] = public_file
            report = self.header
            report['measurements'] = []
            self.rid = settings.db.reports.insert(report)

            test_name = self.header['test_name']

            # Insert each measurement into the database
            for entry in self:
                if self.header["software_name"] not in WHITELIST:
                    entry = run_process(test_name, report_file, entry)
                settings.db.reports.update(
                    {'_id': self.rid},
                    {'$push': {'measurements': entry}
                })

            try:
                makedirs(dirname(public_file))
            except OSError as exc:
                if exc.errno != 17:
                    raise exc

            fsrc = open(report_file, 'rb')
            fdst = gzip.open(public_file, 'wb')
            shutil.copyfileobj(fsrc, fdst)
            fsrc.close()
            fdst.close()

            remove(report_file)
        except Exception, e:
            log.warning("Exception", exc_info=1)
        semaphore.release()
        log.info("Imported %s" % report_file)

    def __iter__(self):
        return self

    def next(self):
        try:
            entry = self._report.next()
        except StopIteration:
            self.fh.close()
            raise StopIteration
        if not entry:
            entry = self.next()
        return entry


def main():

    logfile = join(settings.sanitised_directory, "publish.log")
    fh = logging.FileHandler(logfile)
    log.addHandler(fh)

    manager = Manager()
    semaphore = manager.Semaphore(cpu_count())
    pool = Pool(processes=cpu_count())

    arguments = sys.argv[2:]
    if not arguments:
        report_files = list_report_files(settings.sanitised_directory)
    else:
        report_files = (elem for elem in arguments if elem.endswith(".yamloo"))

    report_counter = 0
    # iterate over report files
    while True:
        try:
            semaphore.acquire()
            report_file = report_files.next()
            log.info("Importing %s" % report_file)
            pool.apply_async(ReportInserter, (report_file, semaphore))
            report_counter += 1

        except StopIteration:
            break

    log.info("Waiting for all the tasks to finish")
    pool.close()
    pool.join()

    log.info("Imported %d reports" % report_counter)

if __name__ == "__main__":
    main()
