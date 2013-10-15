'''Test the stats we keep about queues'''

from common import TestQless


class TestStats(TestQless):
    '''Tests the stats we keep about queues'''
    def test_malformed(self):
        '''Enumerate all the ways to send malformed requests'''
        self.assertMalformed(self.lua, [
            ('stats', 0),
            ('stats', 0, 'queue'),
            ('stats', 0, 'queue', 'foo')
        ])

    def test_wait(self):
        '''It correctly tracks wait times'''
        stats = self.lua('stats', 0, 'queue', 0)
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run']['count'], 0)

        # Put in jobs all at the same time, and then pop them at different
        # times to ensure that we know stats about how long they've waited
        jids = map(str, range(20))
        for jid in jids:
            self.lua('put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        for jid in jids:
            self.lua('pop', jid, 'queue', 'worker', 1)

        stats = self.lua('stats', 0, 'queue', 0)
        self.assertEqual(stats['wait']['count'], 20)
        self.assertAlmostEqual(stats['wait']['mean'], 9.5)
        self.assertAlmostEqual(stats['wait']['std'], 5.916079783099)
        self.assertEqual(stats['wait']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['wait']['histogram']), 20)

    def test_completion(self):
        '''It correctly tracks job run times'''
        # Put in a bunch of jobs and pop them all at the same time, and then
        # we'll complete them at different times and check the computed stats
        jids = map(str, range(20))
        for jid in jids:
            self.lua('put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', 0, 'queue', 'worker', 1)
        for jid in jids:
            self.lua('complete', jid, jid, 'worker', 'queue', {})

        stats = self.lua('stats', 0, 'queue', 0)
        self.assertEqual(stats['run']['count'], 20)
        self.assertAlmostEqual(stats['run']['mean'], 9.5)
        self.assertAlmostEqual(stats['run']['std'], 5.916079783099)
        self.assertEqual(stats['run']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run']['histogram']), 20)

    def test_failed(self):
        '''It correctly tracks failed jobs and failures'''
        # The distinction here between 'failed' and 'failure' is that 'failed'
        # is the number of jobs that are currently failed, as opposed to the
        # number of times a job has failed in that queue
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 1)
        self.lua('fail', 0, 'jid', 'worker', 'group', 'message', {})
        stats = self.lua('stats', 0, 'queue', 0)
        self.assertEqual(stats['failed'], 1)
        self.assertEqual(stats['failures'], 1)

        # If we put the job back in a queue, we don't see any failed jobs,
        # but we still see a failure
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        stats = self.lua('stats', 0, 'queue', 0)
        self.assertEqual(stats['failed'], 0)
        self.assertEqual(stats['failures'], 1)

    def test_failed_cancel(self):
        '''If we fail a job, and then cancel it, stats reflects 0 failed job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 1)
        self.lua('fail', 0, 'jid', 'worker', 'group', 'message', {})
        self.lua('cancel', 0, 'jid')
        stats = self.lua('stats', 0, 'queue', 0)
        self.assertEqual(stats['failed'], 0)
        self.assertEqual(stats['failures'], 1)

    def test_retries(self):
        '''It correctly tracks retries in a queue'''
        self.lua('config.set', 0, 'heartbeat', '-10')
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 1)
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['retries'], 0)
        self.lua('pop', 0, 'queue', 'worker', 1)
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['retries'], 1)

    def test_original_day(self):
        '''It updates stats for the original day of stats'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 1)
        self.lua('fail', 0, 'jid', 'worker', 'group', 'message', {})
        # Put it somehwere 1.5 days later
        self.lua('put', 129600, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['failed'], 0)
        self.assertEqual(self.lua('stats', 0, 'queue', 129600)['failed'], 0)

    def test_failed_retries(self):
        '''It updates stats for jobs failed from retries'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('retry', 3, 'jid', 'queue', 'worker')
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['failed'], 1)
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['failures'], 1)

    def test_failed_pop_retries(self):
        '''Increment the count failed jobs when job fail from retries'''
        '''Can cancel job that has been failed from retries through pop'''
        self.lua('config.set', 0, 'heartbeat', -10)
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('pop', 2, 'queue', 'worker', 10)
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['failed'], 1)
        self.assertEqual(self.lua('stats', 0, 'queue', 0)['failures'], 1)
