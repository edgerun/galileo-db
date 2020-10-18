import abc

from galileodb import ExperimentDatabase, Experiment, Telemetry
from galileodb.model import ExperimentEvent, RequestTrace


class AbstractTestExperimentDatabase(abc.ABC):
    db: ExperimentDatabase

    def test_get_experiment_on_invalid_id(self):
        result = self.db.get_experiment('NOTEXISTS')
        self.assertIsNone(result)

    def test_save_and_get_experiment(self):
        expected = Experiment('expid1', 'test_experiment', 'unittest', 10, 100, 1, 'running')

        self.db.save_experiment(expected)

        actual = self.db.get_experiment('expid1')

        self.assertEqual('expid1', actual.id)
        self.assertEqual('test_experiment', actual.name)
        self.assertEqual('unittest', actual.creator)
        self.assertEqual(10., actual.start)
        self.assertEqual(100., actual.end)
        self.assertEqual(1., actual.created)
        self.assertEqual('running', actual.status)

    def test_update_experiment_and_get(self):
        exp = Experiment('expid8', 'test_experiment', 'unittest', 10, None, 1, 'running')
        self.db.save_experiment(exp)

        self.assertEqual(self.db.get_experiment('expid8').status, 'running')
        self.assertEqual(self.db.get_experiment('expid8').end, None)

        exp.status = 'finished'
        exp.end = 100

        self.db.update_experiment(exp)

        self.assertEqual(self.db.get_experiment('expid8').status, 'finished')
        self.assertEqual(self.db.get_experiment('expid8').end, 100)

    def test_save_and_get_telemetry(self):
        telemetry = [
            Telemetry(1, 'cpu', 'n1', 32, 'expid1'),
            Telemetry(2, 'cpu', 'n1', 33, 'expid1'),
            Telemetry(3, 'cpu', 'n1', 31, 'expid2'),
        ]

        self.db.save_telemetry(telemetry)

        actual = self.db.get_telemetry('expid1')
        self.assertEqual(2, len(actual))
        self.assertEqual(telemetry[0], actual[0])
        self.assertEqual(telemetry[1], actual[1])

        actual = self.db.get_telemetry('expid2')
        self.assertEqual(1, len(actual))
        self.assertEqual(telemetry[2], actual[0])

        actual = self.db.get_telemetry()
        self.assertEqual(3, len(actual))
        self.assertEqual(telemetry[0], actual[0])
        self.assertEqual(telemetry[1], actual[1])
        self.assertEqual(telemetry[2], actual[2])

    def test_save_and_touch_and_get_traces(self):
        traces = [
            RequestTrace('req1', 'c1', 's1', 1.1, 1.2, 1.3, server='h1', status=200),
            RequestTrace('req2', 'c2', 's2', 2.1, 2.2, 2.3, server='h2', status=200),
            RequestTrace('req3', 'c3', 's1', 3.1, 3.2, 3.3, server='h1', status=200, response='time=123'),
            RequestTrace('req4', 'c1', 's1', 4.1, 4.2, 4.3, server='h1', status=200),
        ]

        self.db.save_experiment(Experiment('exp1', start=1, end=3.5, status='FINISHED'))

        self.db.save_traces(traces)

        self.db.touch_traces(self.db.get_experiment('exp1'))  # should touch the first 3

        actual = self.db.get_traces('exp1')

        expected = [
            RequestTrace('req1', 'c1', 's1', 1.1, 1.2, 1.3, server='h1', status=200, exp_id='exp1'),
            RequestTrace('req2', 'c2', 's2', 2.1, 2.2, 2.3, server='h2', status=200, exp_id='exp1'),
            RequestTrace('req3', 'c3', 's1', 3.1, 3.2, 3.3, server='h1', status=200, response='time=123', exp_id='exp1')
        ]

        self.assertEqual(expected, actual)

    def test_save_and_get_events(self):
        events = [
            ExperimentEvent('exp1', 1, 'begin'),
            ExperimentEvent('exp1', 2, 'start', 'function1'),
            ExperimentEvent('exp2', 3, 'stop', 'function1'),
        ]

        self.db.save_events(events)

        stored = self.db.get_events()
        self.assertEqual(3, len(stored))

        self.assertEqual(ExperimentEvent('exp1', 1.0, 'begin', None), stored[0])
        self.assertEqual(ExperimentEvent('exp1', 2.0, 'start', 'function1'), stored[1])
        self.assertEqual(ExperimentEvent('exp2', 3.0, 'stop', 'function1'), stored[2])

    def test_save_and_get_events_for_experiment(self):
        events = [
            ExperimentEvent('exp1', 1, 'begin'),
            ExperimentEvent('exp1', 2, 'start', 'function1'),
            ExperimentEvent('exp2', 3, 'stop', 'function1'),
        ]

        self.db.save_events(events)

        stored = self.db.get_events('exp1')
        self.assertEqual(2, len(stored))

        self.assertEqual(ExperimentEvent('exp1', 1.0, 'begin', None), stored[0])
        self.assertEqual(ExperimentEvent('exp1', 2.0, 'start', 'function1'), stored[1])

    def test_delete_experiment(self):
        exp_id = 'expid10'
        exp_id_control = 'expid11'
        self.db.save_experiment(Experiment(exp_id, 'test_experiment', 'unittest', 10, 100, 1, 'finished'))
        self.db.save_telemetry([Telemetry(1, 'cpu', 'n1', 32, exp_id)])

        self.db.save_experiment(Experiment(exp_id_control, 'test_experiment', 'unittest', 10, 100, 1, 'finished'))
        self.db.save_telemetry([Telemetry(1, 'cpu', 'n1', 32, exp_id_control)])

        self.assertIsNotNone(self.db.get_experiment(exp_id))

        self.db.delete_experiment(exp_id)

        self.assertIsNone(self.db.get_experiment(exp_id))
        self.assertIsNotNone(self.db.get_experiment(exp_id_control))
