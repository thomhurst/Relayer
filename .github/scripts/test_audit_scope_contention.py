import unittest
from audit_scope_contention import audit, BENCHMARK, PROVIDER


def fixture():
    def event(id, time, payload):
        return dict(kind='native-event', id=id, processId=1, threadId=2,
                    timeMilliseconds=time, ProviderName=PROVIDER, payload=payload)
    return [event(1, 0, {'benchmarkName': BENCHMARK}),
            event(15, 1, {'totalOperations': '100'}),
            event(16, 2, {'totalOperations': '100'}),
            event(2, 3, {'benchmarkName': BENCHMARK}),
            dict(kind='coverage', eventsLost=0, samples=0, contentionOnly=True,
                 workloadStarts=1, workloadStops=1, contentionRecords=0)]


class AuditTests(unittest.TestCase):
    def test_zero_events_retains_complete_intervals(self):
        self.assertEqual(audit(fixture())['intervals'], [(1, 2)])

    def test_loss_samples_and_wrong_mode_fail(self):
        for field, value in [('eventsLost', 1), ('samples', 1), ('contentionOnly', False)]:
            with self.subTest(field=field):
                events = fixture()
                events[-1][field] = value
                with self.assertRaises(ValueError):
                    audit(events)

    def test_invalid_marker_coverage_fails(self):
        mutations = [(1, 'threadId', 3), (2, 'processId', 3),
                     (2, 'timeMilliseconds', 4), (2, 'payload', {'totalOperations': '99'}),
                     (0, 'payload', {'benchmarkName': 'Wrong'}),
                     (-1, 'workloadStarts', 2)]
        for index, field, value in mutations:
            with self.subTest(field=field, index=index):
                events = fixture()
                events[index][field] = value
                with self.assertRaises(ValueError):
                    audit(events)
        for index in range(4):
            events = fixture()
            del events[index]
            with self.assertRaises(ValueError):
                audit(events)

    def test_contention_is_retained_without_attribution(self):
        events = fixture()
        native = dict(kind='native-event', ProviderName='Microsoft-Windows-DotNETRuntime',
                      EventName='Contention/Start', processId=1, threadId=9,
                      timeMilliseconds=0.5, payload={'LockID': '123'}, frames=[])
        events.insert(1, native)
        events[-1]['contentionRecords'] = 1
        result = audit(events)
        self.assertEqual(result['events'], [native])
        self.assertIn('unresolved', result['interpretation'])
        events[-1]['contentionRecords'] = 0
        with self.assertRaises(ValueError):
            audit(events)


if __name__ == '__main__':
    unittest.main()
