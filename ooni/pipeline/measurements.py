import logging
from ooni.pipeline import settings


def find_closest(controls, experiment):
    start_time = experiment.get_start_time()
    max_distance_sec = settings.max_distance_control_measurement * 60 * 60

    if len(controls) == 0:
        return None

    best_match = min(controls, key=lambda x: abs(x.get_start_time() - start_time))

    if max_distance_sec == None:
        return best_match
    if max_distance_sec > 0:
        distance_best_match = abs(best_match.get_start_time() - start_time)

        if int(distance_best_match) < max_distance_sec:
            return best_match
        else:
            return None


def truth_table(experiment, control):
    result_experiment = experiment['success']
    result_control = control['success']

    if result_experiment not in [True, False] or result_control not in [True, False]:
        return "invalid"

    if result_experiment and result_control:
        return "ok"
    elif result_experiment and result_control == False:
        return "inconsistent"
    elif result_experiment == False and result_control == True:
        return "blocked"
    elif result_experiment == False and result_control == False:
        return "offline"
    else:
        print experiment, control
        assert True == False


class Measurement(object):

    def __init__(self, measurement, mongodb_client):
        self.measurement = measurement
        self.report_id = measurement['report_id']
        self.mongodb_client = mongodb_client
        self.report = self.mongodb_client.reports.find_one({"_id":
                                                            self.report_id})

    def get_file_url(self):
        report_path = self.report.get('report_file')
        if report_path:
            report_path = '/'.join(report_path.split("/")[-2:])
        else:
            report_path = ""
        return "http://reports.ooni.nu/%s" % report_path

    def get_test_name(self):
        return self.report['test_name']

    def get_country(self):
        return self.report['probe_cc']

    def get_test_input(self):
        return self.measurement['input']

    def get_asn(self):
        return self.report['probe_asn']

    def get_success_value(self):
        return self.measurement.get('connection')

    def get_runtime(self):
        if 'test_runtime' in self.measurement:
            return self.measurement['test_runtime']
        else:
            return self.report.get('test_runtime')

    def get_start_time(self):
        if 'start_time' in self.measurement:
            return self.measurement['start_time']
        else:
            return self.report.get('start_time')

    def is_bridge_reachability(self):
        return self.report['test_name'] == 'bridge_reachability'

    def is_tcpconnect(self):
        return self.report['test_name'] == 'tcp_connect'

    def scrub(self):
        # This is private data of mongodb
        del self.measurement['_id']
        del self.measurement['report_id']

    def add_status_field(self, controls):
        """ Iterate measurements and embed the status field."""
        closest_control = find_closest(controls, self)

        if closest_control == None:
            self.measurement['status'] = "inconclusive"
        else:
            status = truth_table(self.measurement, closest_control.measurement)
            self.measurement['status'] = status

    def add_start_time(self):
        self.measurement['start_time'] = self.get_start_time()
        self.measurement['test_runtime'] = self.get_runtime()

    def add_file_url(self):
        self.measurement['file_url'] = self.get_file_url()

    def add_tcp_connect_field(self, tcp_connects):
        # Let's see if there is a corresponding TCP connect
        # measurement for this bridge reachability measurement

        candidate_measurements_list = []

        for measurement in [x for x in tcp_connects]:
            assert(measurement.get_test_name() == "tcp_connect")
            if measurement.get_asn() == self.get_asn():
                logging.debug("Found potential TCPConnect match: %s %s",
                              measurement.measurement, self.measurement)
                candidate_measurements_list.append(measurement)

        if len(candidate_measurements_list) == 0:
            self.measurement['tcp_connect_success'] = None
            self.measurement['tcp_connect_start_time'] = None
            self.measurement['tcp_connect_runtime'] = None
            return False

        # Now we should have a list of measurements that match the
        # test input and AS. Now we need to find the closest in time.
        closest_tcpconnect = find_closest(candidate_measurements_list, self)

        logging.debug("=====START===")
        logging.debug("For %s", str(self))
        logging.debug("and %s", str(self.report))
        logging.debug("Found closest match: %s", str(closest_tcpconnect))
        logging.debug("with %s", str(closest_tcpconnect.report))
        logging.debug("=====END=====")

        self.measurement[
            'tcp_connect_success'] = closest_tcpconnect.get_success_value()
        self.measurement[
            'tcp_connect_start_time'] = closest_tcpconnect.get_start_time()
        self.measurement[
            'tcp_connect_runtime'] = closest_tcpconnect.get_runtime()
        return True

    def __str__(self):
        return str(self.measurement)


class Measurements(object):

    def __init__(self, measurements, db):
        self.measurements = []
        self.db = db
        for measurement in measurements:
            self.add_measurement(measurement)

    def add_measurement(self, measurement):
        self.measurements.append(Measurement(measurement, self.db))

    def get_experiments(self):
        # Experiments is a map from a country code to an experiment
        # measurement.
        experiments = {}
        for measurement in self.measurements:
            if not measurement.is_bridge_reachability():
                continue

            country = measurement.get_country()
            if country != 'NL':
                if country not in experiments:
                    experiments[country] = []
                experiments[country].append(measurement)
        return experiments

    def get_tcp_connects(self):
        """
        Return a dictionary from an input to a list of tcpconnect
        measurements.
        """
        input_hashes_list = set()
        for measurement in self.measurements:
            try:
                input_hashes_list.add(measurement.report['input_hashes'][0])
            except:
                pass

        tcp_connects = []
        for tcp_connect in self.db.reports.find({
            "test_name": "tcp_connect",
            "input_hashes": {'$in': [[x] for x in input_hashes_list]}
        }):
            for measurement in self.db.measurements.find({
                "report_id": tcp_connect['_id']
            }):
                tcp_connects.append(Measurement(measurement, self.db))
        return tcp_connects

    def get_controls_list(self):
        controls = []
        for measurement in self.measurements:
            if not measurement.is_bridge_reachability():
                continue

            country = measurement.get_country()
            if country == 'NL':
                controls.append(measurement)
        return controls

    def __iter__(self):
        return self

    def next(self):
        for measurement in self.measurements:
            yield measurement
        raise StopIteration
