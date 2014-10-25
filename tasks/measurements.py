
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

    def get_start_time(self):
        if 'start_time' in self.measurement:
            return self.measurement['start_time']
        else:
            return self.report['start_time']

    def is_bridge_reachability(self):
        return self.report['test_name'] == 'bridge_reachability'

    def scrub(self):
        # This is private data of mongodb
        del self.measurement['_id']
        del self.measurement['report_id']

    def add_status_field(self, controls):
        """ Iterate measurements and embed the status field."""
        closest_control = find_closest(controls, self)
        status = truth_table(self.measurement, closest_control.measurement)
        self.measurement['status'] = status

    def add_tcp_connect_field(self):
        if self.report['test_name'] == 'tcp_connect':
            pass


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
