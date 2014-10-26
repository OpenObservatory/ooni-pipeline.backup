import logging

def find_closest(controls, experiment):
    start_time = experiment.get_start_time()
    return min(controls, key=lambda x: abs(x.get_start_time() - start_time))


def truth_table(experiment, control):
    result_experiment = experiment['success']
    result_control = control['success']

    if result_experiment == True and result_control == True:
        return "ok"
    elif result_experiment == True and result_control == False:
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
        self.report = self.mongodb_client.reports.find_one({"_id": self.report_id})

    def get_test_name(self):
        return self.report['test_name']

    def get_country(self):
        return self.report['probe_cc']

    def get_test_input(self):
        return self.measurement['input']

    def get_asn(self):
        return self.report['probe_asn']

    def get_success_value(self):
        return self.measurement['success']

    def get_runtime(self):
        if 'test_runtime' in self.measurement:
            return self.measurement['test_runtime']
        elif 'test_runtime' in self.report:
            return self.report['test_runtime']
        else:
            return None

    def get_start_time(self):
        if 'start_time' in self.measurement:
            return self.measurement['start_time']
        else:
            return self.report['start_time']

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
        status = truth_table(self.measurement, closest_control.measurement)
        self.measurement['status'] = status

    def add_tcpconnect_field(self, tcpconnects):
        # Let's see if there is a corresponding TCP connect
        # measurement for this bridge reachability measurement

        candidate_measurements_list = []

        for test_input, measurements in tcpconnects.items():

            # First filter by test input
            if test_input != self.get_test_input():
                continue

            # Now loop over all the candidate tcpconnect measurements
            # and filter by ASN.
            for measurement in measurements:
                assert(measurement.get_test_name() == 'tcp_connect')

                if measurement.get_asn() == self.get_asn():
                    logging.debug("Found potential TCPConnect match: %s %s",
                                  measurement.measurement, self.measurement)
                    candidate_measurements_list.append(measurement)

        # Now we should have a list of measurements that match the
        # test input and AS. Now we need to find the closest in time.
        closest_tcpconnect = find_closest(candidate_measurements_list, self)

        logging.debug("=====START===")
        logging.debug("For %s", str(self))
        logging.debug("and %s", str(self.report))
        logging.debug("Found closest match: %s", str(closest_tcpconnect))
        logging.debug("with %s", str(closest_tcpconnect.report))
        logging.debug("=====END=====")

        self.measurement['tcp_connect_success'] = closest_tcpconnect.get_success_value()
        self.measurement['tcp_connect_start_time'] = closest_tcpconnect.get_start_time()
        self.measurement['tcp_connect_runtime'] = closest_tcpconnect.get_runtime()

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

    def get_tcpconnects(self):
        """
        Return a dictionary from an input to a list of tcpconnect
        measurements.
        """
        tcpconnects = {}

        for measurement in self.measurements:
            test_input = measurement.get_test_input()

            if measurement.is_tcpconnect():
                if test_input not in tcpconnects:
                    tcpconnects[test_input] = []

                tcpconnects[test_input].append(measurement)

        return tcpconnects

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
