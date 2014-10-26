import unittest
from os.path import dirname, realpath, join, isfile, exists
from os import mkdir
import os
import sys
import shutil
import subprocess
from tempfile import mkdtemp

class TestIntegrationSanitise(unittest.TestCase):
    # Currently is not possible to create proper unit tests for sanitise.py,
    # it needs to be modularised first
    # TODO: there should be more tests for the cases:
    #       * archive and/or santised paths already exist
    #       * check ips in path_to_ip_port_based_mapping.json are not present
    #          in the sanitised file
    #       * check the hashes in the sanitised file
    def setUp(self):
        """
        Copy tests/testdatasanitise to a temporary directory
        and run sanitise.py with the teporary directory
        """

        base_path = realpath(join(dirname(__file__), '..'))

        report_path = 'reports/'
        sanitised_path = 'sanitised/'
        archive_path = 'archive/'

        report_file = '2012-01-01T120000Z_AS3.yamloo'
        report_file_path = 'reports/IT/2012-01-01T120000Z_AS3.yamloo'
        bridge_db_mapping_file_path = 'path_to_ip_port_based_mapping.json'

        test_base_path = realpath(dirname(__file__))
        exampledata_path = join(test_base_path, 'testdatasanitise')

        self.test_data_path = mkdtemp()

        self.test_report_path = join(self.test_data_path, report_path)
        self.test_sanitised_path = join(self.test_data_path, sanitised_path)
        self.test_archive_path = join(self.test_data_path, archive_path)

        self.test_bridge_db_mapping_file_path = join(self.test_data_path,
                bridge_db_mapping_file_path)
        self.test_report_file_path = join(self.test_data_path,
                report_file_path)
        self.test_sanitised_file_path = join(self.test_sanitised_path,
                report_file)
        self.test_archive_file_path = join(self.test_archive_path,
                report_file + '.gz')

        # copy source example reports
        shutil.copytree(join(exampledata_path, report_path),
                self.test_report_path)

        # copy ip map file
        shutil.copy(join(exampledata_path, bridge_db_mapping_file_path),
                self.test_bridge_db_mapping_file_path)

        # create destiny directories
        for dst_path in [self.test_sanitised_path, self.test_archive_path]:
            mkdir(dst_path)

        sys.path.insert(0, base_path)
        from ooni.pipeline.task.sanitise import main
        from ooni.pipeline import settings
        import json
        # overwrite settings
        settings.archive_directory = self.test_archive_path
        settings.reports_directory = self.test_report_path
        settings.bridge_db_mapping_file = self.test_bridge_db_mapping_file_path
        settings.sanitised_directory = self.test_sanitised_path
        try:
            settings.bridge_db_mapping = json.load(
                    open(settings.bridge_db_mapping_file))
        except:
            settings.bridge_db_mapping = None
        main()


    def test_santise(self):
        """
        """
        # check sanitised file is created
        self.assertTrue(isfile(self.test_sanitised_file_path))
        # check archive file is created
        self.assertTrue(isfile(self.test_archive_file_path))
        # check report file is deleted
        self.assertFalse(isfile(self.test_report_file_path))

        # FIXME: check none of the ips in ipmap is present in sanitised file
        # here bridge_reachability_tcp_connect could be used


    def tearDown(self):
        """
        remove the tests/testdata directory
        """
        shutil.rmtree(self.test_data_path)


if __name__ == '__main__':
    unittest.main()
