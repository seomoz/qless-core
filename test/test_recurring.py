'''Tests for recurring jobs'''

from common import TestQless


class TestRecurring(TestQless):
    '''Tests for recurring jobs'''
    def test_malformed(self):
        '''Enumerate all the malformed possibilities'''
        self.assertMalformed(self.lua, [
            ('recur', 0),
            ('recur', 0, 'queue'),
            ('recur', 0, 'queue', 'jid'),
            ('recur', 0, 'queue', 'jid', 'klass'),
            ('recur', 0, 'queue', 'jid', 'klass', {}),
            ('recur', 0, 'queue', 'jid', 'klass', '[}'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'foo'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 'foo'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 'foo'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'tags'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'tags', '[}'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'priority'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'priority', 'foo'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'retries'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'retries', 'foo'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'backlog'),
            ('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
                'backlog', 'foo'),
        ])

        # In order for these tests to work, there must be a job
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertMalformed(self.lua, [
            ('recur.update', 0, 'jid', 'priority'),
            ('recur.update', 0, 'jid', 'priority', 'foo'),
            ('recur.update', 0, 'jid', 'interval'),
            ('recur.update', 0, 'jid', 'interval', 'foo'),
            ('recur.update', 0, 'jid', 'retries'),
            ('recur.update', 0, 'jid', 'retries', 'foo'),
            ('recur.update', 0, 'jid', 'data'),
            ('recur.update', 0, 'jid', 'data', '[}'),
            ('recur.update', 0, 'jid', 'klass'),
            ('recur.update', 0, 'jid', 'queue'),
            ('recur.update', 0, 'jid', 'backlog'),
            ('recur.update', 0, 'jid', 'backlog', 'foo')
        ])

    def test_basic(self):
        '''Simple recurring jobs'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        # Pop off the first recurring job
        popped = self.lua('pop', 0, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-1')

        # If we wait 59 seconds, there won't be a job, but at 60, yes
        popped = self.lua('pop', 59, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)
        popped = self.lua('pop', 61, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-2')

    def test_offset(self):
        '''We can set an offset from now for jobs to recur on'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 10)
        # There shouldn't be any jobs available just yet
        popped = self.lua('pop', 9, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)
        popped = self.lua('pop', 11, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-1')

        # And now it recurs normally
        popped = self.lua('pop', 69, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)
        popped = self.lua('pop', 71, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-2')

    def test_tags(self):
        '''Recurring jobs can be given tags'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
            'tags', ['foo', 'bar'])
        job = self.lua('pop', 0, 'queue', 'worker', 10)[0]
        self.assertEqual(job['tags'], ['foo', 'bar'])

    def test_priority(self):
        '''Recurring jobs can be given priority'''
        # Put one job with low priority
        self.lua('put', 0, 'worker', 'queue', 'low', 'klass', {}, 0, 'priority', 0)
        self.lua('recur', 0, 'queue', 'high', 'klass', {},
            'interval', 60, 0, 'priority', 10)
        jobs = self.lua('pop', 0, 'queue', 'worker', 10)
        # We should see high-1 and then low
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['jid'], 'high-1')
        self.assertEqual(jobs[0]['priority'], 10)
        self.assertEqual(jobs[1]['jid'], 'low')

    def test_retries(self):
        '''Recurring job retries are passed on to child jobs'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {},
            'interval', 60, 0, 'retries', 2)
        job = self.lua('pop', 0, 'queue', 'worker', 10)[0]
        self.assertEqual(job['retries'], 2)
        self.assertEqual(job['remaining'], 2)

    def test_backlog(self):
        '''Recurring jobs can limit the number of jobs they spawn'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {},
            'interval', 60, 0, 'backlog', 1)
        jobs = self.lua('pop', 600, 'queue', 'worker', 10)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['jid'], 'jid-1')

    def test_get(self):
        '''We should be able to get recurring jobs'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(self.lua('recur.get', 0, 'jid'), {
            'backlog': 0,
            'count': 0,
            'data': '{}',
            'interval': 60,
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'retries': 0,
            'state': 'recur',
            'tags': {}
        })

    def test_update_priority(self):
        '''We need to be able to update recurring job attributes'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['priority'], 0)
        self.lua('recur.update', 0, 'jid', 'priority', 10)
        self.assertEqual(
            self.lua('pop', 60, 'queue', 'worker', 10)[0]['priority'], 10)

    def test_update_interval(self):
        '''We need to be able to update the interval'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(len(self.lua('pop', 0, 'queue', 'worker', 10)), 1)
        self.lua('recur.update', 0, 'jid', 'interval', 10)
        self.assertEqual(len(self.lua('pop', 60, 'queue', 'worker', 10)), 6)

    def test_update_retries(self):
        '''We need to be able to update the retries'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {},
            'interval', 60, 0, 'retries', 5)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['retries'], 5)
        self.lua('recur.update', 0, 'jid', 'retries', 2)
        self.assertEqual(
            self.lua('pop', 60, 'queue', 'worker', 10)[0]['retries'], 2)

    def test_update_data(self):
        '''We need to be able to update the data'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60,  0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['data'], '{}')
        self.lua('recur.update', 0, 'jid', 'data', {'foo': 'bar'})
        self.assertEqual(self.lua(
            'pop', 60, 'queue', 'worker', 10)[0]['data'], '{"foo": "bar"}')

    def test_update_klass(self):
        '''We need to be able to update klass'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['klass'], 'klass')
        self.lua('recur.update', 0, 'jid', 'klass', 'class')
        self.assertEqual(
            self.lua('pop', 60, 'queue', 'worker', 10)[0]['klass'], 'class')

    def test_update_queue(self):
        '''Need to be able to move the recurring job to another queue'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(len(self.lua('pop', 0, 'queue', 'worker', 10)), 1)
        self.lua('recur.update', 0, 'jid', 'queue', 'other')
        # No longer available in the old queue
        self.assertEqual(len(self.lua('pop', 60, 'queue', 'worker', 10)), 0)
        self.assertEqual(len(self.lua('pop', 60, 'other', 'worker', 10)), 1)

    def test_unrecur(self):
        '''Stop a recurring job'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(len(self.lua('pop', 0, 'queue', 'worker', 10)), 1)
        self.lua('unrecur', 0, 'jid')
        self.assertEqual(len(self.lua('pop', 60, 'queue', 'worker', 10)), 0)

    def test_empty_array_data(self):
        '''Empty array of data is preserved'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', [], 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['data'], '[]')

    def test_multiple(self):
        '''If multiple intervals have passed, then returns multiple jobs'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            len(self.lua('pop', 599, 'queue', 'worker', 10)), 10)

    def test_tag(self):
        '''We should be able to add tags to jobs'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['tags'], {})
        self.lua('recur.tag', 0, 'jid', 'foo')
        self.assertEqual(
            self.lua('pop', 60, 'queue', 'worker', 10)[0]['tags'], ['foo'])

    def test_untag(self):
        '''We should be able to remove tags from a job'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {},
            'interval', 60, 0, 'tags', ['foo'])
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['tags'], ['foo'])
        self.lua('recur.untag', 0, 'jid', 'foo')
        self.assertEqual(
            self.lua('pop', 60, 'queue', 'worker', 10)[0]['tags'], {})

    def test_rerecur(self):
        '''Don't reset the jid counter when re-recurring a job'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['jid'], 'jid-1')
        # Re-recur it
        self.lua('recur', 60, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 60, 'queue', 'worker', 10)[0]['jid'], 'jid-2')

    def test_rerecur_attributes(self):
        '''Re-recurring a job updates its attributes'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0,
            'priority', 10, 'tags', ['foo'], 'retries', 2)
        self.assertEqual(self.lua('pop', 0, 'queue', 'worker', 10)[0], {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 60,
            'failure': {},
            'history': [{'q': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 0, 'worker': 'worker'}],
            'jid': 'jid-1',
            'klass': 'klass',
            'priority': 10,
            'queue': 'queue',
            'remaining': 2,
            'retries': 2,
            'state': 'running',
            'tags': ['foo'],
            'tracked': False,
            'worker': 'worker'})
        self.lua('recur', 60, 'queue', 'jid', 'class', {'foo': 'bar'},
            'interval', 10, 0, 'priority', 5, 'tags', ['bar'], 'retries', 5)
        self.assertEqual(self.lua('pop', 60, 'queue', 'worker', 10)[0], {
            'data': '{"foo": "bar"}',
            'dependencies': {},
            'dependents': {},
            'expires': 120,
            'failure': {},
            'history': [{'q': 'queue', 'what': 'put', 'when': 60},
                        {'what': 'popped', 'when': 60, 'worker': 'worker'}],
            'jid': 'jid-2',
            'klass': 'class',
            'priority': 5,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'running',
            'tags': ['bar'],
            'tracked': False,
            'worker': 'worker'})

    def test_rerecur_move(self):
        '''Re-recurring a job in a new queue works like a move'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 0, 'queue', 'worker', 10)[0]['jid'], 'jid-1')
        self.lua('recur', 60, 'other', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(
            self.lua('pop', 60, 'other', 'worker', 10)[0]['jid'], 'jid-2')

    def test_history(self):
        '''Spawned jobs are 'put' at the time they would have been scheduled'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        jobs = self.lua('pop', 599, 'queue', 'worker', 100)
        times = [job['history'][0]['when'] for job in jobs]
        self.assertEqual(
            times, [0, 60, 120, 180, 240, 300, 360, 420, 480, 540])
