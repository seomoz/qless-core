'''Tests about locks'''

import redis
from common import TestQless


class TestLocks(TestQless):
    '''Locks tests'''
    def test_malformed(self):
        '''Enumerate malformed inputs into heartbeat'''
        self.assertMalformed(self.lua, [
            ('heartbeat', 0),
            ('heartbeat', 0, 'jid'),
            ('heartbeat', 0, 'jid', 'worker', '[}')
        ])

    def setUp(self):
        TestQless.setUp(self)
        # No grace period for any of these tests
        self.lua('config.set', 0, 'grace-period', 0)

    def test_move(self):
        '''Moving ajob should expire any existing locks'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('heartbeat', 2, 'jid', 'worker', {})
        # Move the job after it's been popped
        self.lua('put', 3, 'worker', 'other', 'jid', 'klass', {}, 0)
        # Now this job cannot be heartbeated
        self.assertRaisesRegexp(redis.ResponseError, r'waiting',
            self.lua, 'heartbeat',  4, 'jid', 'worker', {})

    def test_lose_lock(self):
        '''When enough time passes, we lose our lock on a job'''
        # Put and pop a job
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('pop', 1, 'queue', 'worker', 10)[0]
        # No jobs should be available since the lock is still valid
        self.assertEqual(self.lua('pop', 2, 'queue', 'worker', 10), {})
        self.assertEqual(self.lua(
            'pop', job['expires'] + 10, 'queue', 'another', 10), [{
                'data': '{}',
                'dependencies': {},
                'dependents': {},
                'expires': 131,
                'failure': {},
                'history': [
                    {'q': 'queue', 'what': 'put', 'when': 0},
                    {'what': 'popped', 'when': 1, 'worker': 'worker'},
                    {'what': 'timed-out', 'when': 71},
                    {'what': 'popped', 'when': 71, 'worker': 'another'}],
                'jid': 'jid',
                'klass': 'klass',
                'priority': 0,
                'queue': 'queue',
                'remaining': 4,
                'retries': 5,
                'state': 'running',
                'tags': {},
                'tracked': False,
                'worker': 'another'}])
        # When we try to heartbeat, it should raise an exception
        self.assertRaisesRegexp(redis.ResponseError, r'given out to another',
            self.lua, 'heartbeat', 1000, 'jid', 'worker', {})

    def test_heartbeat(self):
        '''Heartbeating extends the lock'''
        # Put and pop a job
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('pop', 1, 'queue', 'worker', 10)[0]
        # No jobs should be available since the lock is still valid
        self.assertEqual(self.lua('pop', 2, 'queue', 'worker', 10), {})
        # We should see our expiration update after a heartbeat
        self.assertTrue(
            self.lua('heartbeat', 3, 'jid', 'worker', {}) > job['expires'])

    def test_heartbeat_waiting(self):
        '''Only popped jobs can be heartbeated'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'waiting',
            self.lua, 'heartbeat',  1, 'jid', 'worker', {})
        # Pop is and it should work
        self.lua('pop', 2, 'queue', 'worker', 10)
        self.lua('heartbeat', 3, 'jid', 'worker', {})

    def test_heartbeat_failed(self):
        '''Cannot heartbeat a failed job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 0, 'jid', 'worker', 'foo', 'bar', {})
        self.assertRaisesRegexp(redis.ResponseError, r'failed',
            self.lua, 'heartbeat',  0, 'jid', 'worker', {})

    def test_heartbeat_depends(self):
        '''Cannot heartbeat a dependent job'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'depends',
            self.lua, 'heartbeat',  0, 'b', 'worker', {})

    def test_heartbeat_scheduled(self):
        '''Cannot heartbeat a scheduled job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertRaisesRegexp(redis.ResponseError, r'scheduled',
            self.lua, 'heartbeat',  0, 'jid', 'worker', {})

    def test_heartbeat_nonexistent(self):
        '''Cannot heartbeat a job that doesn't exist'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'heartbeat',  0, 'jid', 'worker', {})
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('heartbeat', 0, 'jid', 'worker', {})

    def test_heartbeat_completed(self):
        '''Cannot heartbeat a job that has been completed'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('complete', 0, 'jid', 'worker', 'queue', {})
        self.assertRaisesRegexp(redis.ResponseError, r'complete',
            self.lua, 'heartbeat',  0, 'jid', 'worker', {})

    def test_heartbeat_wrong_worker(self):
        '''Only the worker with a job's lock can heartbeat it'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        # Another worker can't heartbeat, but we can
        self.assertRaisesRegexp(redis.ResponseError, r'another worker',
            self.lua, 'heartbeat',  2, 'jid', 'another', {})
        self.lua('heartbeat', 2, 'jid', 'worker', {})


class TestRetries(TestQless):
    '''Test all the behavior surrounding retries'''
    def setUp(self):
        TestQless.setUp(self)
        # No grace periods for this
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('config.set', 0, 'heartbeat', -10)

    def test_basic(self):
        '''The retries and remaining counters are decremented appropriately'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 5)
        self.lua('pop', 0, 'queue', 'worker', 10)
        job = self.lua('pop', 0, 'queue', 'another', 10)[0]
        self.assertEqual(job['retries'], 5)
        self.assertEqual(job['remaining'], 4)

    def test_move_failed_retries(self):
        '''Can move a job even if it's failed retries'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.assertEqual(self.lua('pop', 0, 'queue', 'worker', 10), {})
        self.assertEqual(self.lua('get', 0, 'jid')['state'], 'failed')
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('get', 0, 'jid')['state'], 'waiting')

    def test_reset_complete(self):
        '''Completing a job resets its retries counter'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 5)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua(
            'complete', 0, 'jid', 'worker', 'queue', {}, 'next', 'queue')
        self.assertEqual(self.lua(
            'pop', 0, 'queue', 'worker', 10)[0]['remaining'], 5)

    def test_reset_move(self):
        '''Moving a job resets its retries counter'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 5)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('pop', 0, 'queue', 'worker', 10)
        # Re-put the job without specifying retries
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua(
            'pop', 0, 'queue', 'worker', 10)[0]['remaining'], 5)


class TestRetry(TestQless):
    '''Test all the behavior surrounding retry'''
    maxDiff = 100000
    def test_malformed(self):
        '''Enumerate all the malformed inputs'''
        self.assertMalformed(self.lua, [
            ('retry', 0),
            ('retry', 0, 'jid'),
            ('retry', 0, 'jid', 'queue'),
            ('retry', 0, 'jid', 'queue', 'worker'),
            ('retry', 0, 'jid', 'queue', 'worker', 'foo'),
        ])
        # function QlessJob:retry(now, queue, worker, delay, group, message)

    def test_retry_waiting(self):
        '''Cannot retry a job that's waiting'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'retry', 0, 'jid', 'queue', 'worker', 0)

    def test_retry_completed(self):
        '''Cannot retry a completed job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('complete', 0, 'jid', 'worker', 'queue', {})
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'retry', 0, 'jid', 'queue', 'worker', 0)

    def test_retry_failed(self):
        '''Cannot retry a failed job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 0, 'jid', 'worker', 'group', 'message', {})
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'retry', 0, 'jid', 'queue', 'worker', 0)

    def test_retry_otherowner(self):
        '''Cannot retry a job owned by another worker'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another worker',
            self.lua, 'retry', 0, 'jid', 'queue', 'another', 0)

    def test_retry_complete(self):
        '''Cannot complete a job immediately after retry'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('retry', 0, 'jid', 'queue', 'worker', 0)
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'complete', 0, 'jid', 'worker', 'queue', {})

    def test_retry_fail(self):
        '''Cannot fail a job immediately after retry'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('retry', 0, 'jid', 'queue', 'worker', 0)
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'fail', 0, 'jid', 'worker', 'group', 'message', {})

    def test_retry_heartbeat(self):
        '''Cannot heartbeat a job immediately after retry'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('retry', 0, 'jid', 'queue', 'worker', 0)
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'heartbeat', 0, 'jid', 'worker', {})

    def test_retry_nonexistent(self):
        '''It's an error to retry a nonexistent job'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'retry', 0, 'jid', 'queue', 'another', 0)

    def test_retry_group_message(self):
        '''Can provide a group/message to be used for retries'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua(
            'retry', 0, 'jid', 'queue', 'worker', 0, 'group', 'message')
        self.assertEqual(self.lua('get', 0, 'jid'), {'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {'group': 'group',
                        'message': 'message',
                        'when': 0,
                        'worker': 'worker'},
            'history': [{'q': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 0, 'worker': 'worker'},
                        {'group': 'group', 'what': 'failed', 'when': 0}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': -1,
            'retries': 0,
            'state': 'failed',
            'tags': {},
            'tracked': False,
            'worker': u''})

    def test_retry_delay(self):
        '''Can retry a job with a delay and then it's considered scheduled'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua(
            'retry', 0, 'jid', 'queue', 'worker', 10)
        # Now it should be considered scheduled
        self.assertEqual(self.lua('pop', 0, 'queue', 'worker', 10), {})
        self.assertEqual(self.lua('get', 0, 'jid')['state'], 'scheduled')

    def test_retry_wrong_queue(self):
        '''Cannot retry a job in the wrong queue'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('retry', 0, 'jid', 'queue', 'worker', 0)
        self.assertRaisesRegexp(redis.ResponseError, r'not currently running',
            self.lua, 'heartbeat', 0, 'jid', 'worker', {})

    def test_retry_failed_retries(self):
        '''Retry can be invoked enough to cause it to fail retries'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua(
            'retry', 0, 'jid', 'queue', 'worker', 0)
        self.assertEqual(self.lua('get', 0, 'jid'), {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {
                'group': 'failed-retries-queue',
                'message': 'Job exhausted retries in queue "queue"',
                'when': 0,
                'worker': u''},
            'history': [
                {'q': 'queue', 'what': 'put', 'when': 0},
                {'what': 'popped', 'when': 0, 'worker': 'worker'},
                {'group': 'failed-retries-queue', 'what': 'failed', 'when': 0}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': -1,
            'retries': 0,
            'state': 'failed',
            'tags': {},
            'tracked': False,
            'worker': u''
        })


class TestGracePeriod(TestQless):
    '''Make sure the grace period is honored'''
    # Our grace period for the tests
    grace = 10

    def setUp(self):
        TestQless.setUp(self)
        # Ensure whe know what the grace period is
        self.lua('config.set', 0, 'grace-period', self.grace)

    def test_basic(self):
        '''The lock must expire, and then the grace period must pass'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('pop', 1, 'queue', 'worker', 10)[0]
        # Now, we'll lose the lock, but we should only get a warning, and not
        # actually have the job handed off to another yet
        expires = job['expires'] + 10
        self.assertEqual(self.lua('pop', expires, 'queue', 'another', 10), {})
        # However, once the grace period passes, we should be fine
        self.assertNotEqual(
            self.lua('pop', expires + self.grace, 'queue', 'another', 10), {})

    def test_repeated(self):
        '''Grace periods should be given for each lock lost, not just first'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 20)
        job = self.lua('pop', 0, 'queue', 'worker', 10)[0]
        for _ in xrange(10):
            # Now, we'll lose the lock, but we should only get a warning, and
            # not actually have the job handed off to another yet
            expires = job['expires'] + 10
            self.assertEqual(
                self.lua('pop', expires, 'queue', 'worker', 10), {})
            # However, once the grace period passes, we should be fine
            job = self.lua(
                'pop', expires + self.grace, 'queue', 'worker', 10)[0]

    def test_fail(self):
        '''Can still fail a job during the grace period'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('pop', 0, 'queue', 'worker', 10)[0]
        # Lose the lock and fail the job
        expires = job['expires'] + 10
        self.lua('pop', expires, 'queue', 'worker', 10)
        self.lua('fail', expires, 'jid', 'worker', 'foo', 'bar', {})
        # And make sure that no job is available after the grace period
        self.assertEqual(
            self.lua('pop', expires + self.grace, 'queue', 'worker', 10), {})
