'''Tests about failing jobs'''

import redis
from common import TestQless


class TestFail(TestQless):
    '''Test the behavior of failing jobs'''
    def test_malformed(self):
        '''Enumerate all the malformed cases'''
        self.assertMalformed(self.lua, [
            ('fail', 0),
            ('fail', 0, 'jid'),
            ('fail', 0, 'jid', 'worker'),
            ('fail', 0, 'jid', 'worker', 'group'),
            ('fail', 0, 'jid', 'worker', 'group', 'message'),
            ('fail', 0, 'jid', 'worker', 'group', 'message', '[}')
        ])

    def test_basic(self):
        '''Fail a job in a very basic way'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.assertEqual(self.lua('get', 3, 'jid'), {'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {'group': 'group',
                        'message': 'message',
                        'when': 2,
                        'worker': 'worker'},
            'history': [{'q': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 1, 'worker': 'worker'},
                        {'group': 'group',
                         'what': 'failed',
                         'when': 2,
                         'worker': 'worker'}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'failed',
            'tags': {},
            'tracked': False,
            'worker': u''})

    def test_put(self):
        '''Can put a job that has been failed'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.lua('put', 3, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(len(self.lua('peek', 4, 'queue', 10)), 1)

    def test_fail_waiting(self):
        '''Only popped jobs can be failed'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'waiting',
            self.lua, 'fail', 1, 'jid', 'worker', 'group', 'message', {})
        # Pop is and it should work
        self.lua('pop', 2, 'queue', 'worker', 10)
        self.lua('fail', 3, 'jid', 'worker', 'group', 'message', {})

    def test_fail_depends(self):
        '''Cannot fail a dependent job'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'depends',
            self.lua, 'fail', 1, 'b', 'worker', 'group', 'message', {})

    def test_fail_scheduled(self):
        '''Cannot fail a scheduled job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertRaisesRegexp(redis.ResponseError, r'scheduled',
            self.lua, 'fail', 1, 'jid', 'worker', 'group', 'message', {})

    def test_fail_nonexistent(self):
        '''Cannot fail a job that doesn't exist'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 1, 'jid', 'worker', 'group', 'message', {})

    def test_fail_completed(self):
        '''Cannot fail a job that has been completed'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('complete', 0, 'jid', 'worker', 'queue', {})
        self.assertRaisesRegexp(redis.ResponseError, r'complete',
            self.lua, 'fail', 1, 'jid', 'worker', 'group', 'message', {})

    def test_fail_owner(self):
        '''Cannot fail a job that's running with another worker'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('put', 2, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 3, 'queue', 'another-worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another worker',
            self.lua, 'fail', 4, 'jid', 'worker', 'group', 'message', {})


class TestFailed(TestQless):
    '''Test access to our failed jobs'''
    def test_malformed(self):
        '''Enumerate all the malformed requests'''
        self.assertMalformed(self.lua, [
            ('failed', 0, 'foo', 'foo'),
            ('failed', 0, 'foo', 0, 'foo')
        ])

    def test_basic(self):
        '''We can keep track of failed jobs'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 0, 'jid', 'worker', 'group', 'message')
        self.assertEqual(self.lua('failed', 0), {
            'group': 1
        })
        self.assertEqual(self.lua('failed', 0, 'group'), {
            'total': 1,
            'jobs': ['jid']
        })

    def test_retries(self):
        '''Jobs that fail because of retries should show up'''
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        job = self.lua('pop', 0, 'queue', 'worker', 10)[0]
        self.lua('pop', job['expires'] + 10, 'queue', 'worker', 10)
        self.assertEqual(self.lua('failed', 0), {
            'failed-retries-queue': 1
        })
        self.assertEqual(self.lua('failed', 0, 'failed-retries-queue'), {
            'total': 1,
            'jobs': ['jid']
        })

    def test_failed_pagination(self):
        '''Failed provides paginated access'''
        jids = map(str, range(100))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', jid, 'queue', 'worker', 10)
            self.lua('fail', jid, jid, 'worker', 'group', 'message')
        # Get two pages of 50 and make sure they're what we expect
        jids = list(reversed(jids))
        self.assertEqual(
            self.lua('failed', 0, 'group',  0, 50)['jobs'], jids[:50])
        self.assertEqual(
            self.lua('failed', 0, 'group', 50, 50)['jobs'], jids[50:])


class TestUnfailed(TestQless):
    '''Test access to unfailed'''
    def test_basic(self):
        '''We can unfail in a basic way'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', 0, 'queue', 'worker', 10)
            self.lua('fail', 0, jid, 'worker', 'group', 'message')
            self.assertEqual(self.lua('get', 0, jid)['state'], 'failed')
        self.lua('unfail', 0, 'queue', 'group', 100)
        for jid in jids:
            self.assertEqual(self.lua('get', 0, jid)['state'], 'waiting')
