'''Test all the tracking'''

import redis
from common import TestQless


class TestTrack(TestQless):
    '''Test our tracking abilities'''
    def test_malfomed(self):
        '''Enumerate all the ways that it can be malformed'''
        self.assertMalformed(self.lua, [
            ('track', 0, 'track'),
            ('track', 0, 'untrack'),
            ('track', 0, 'foo')
        ])

    def test_track(self):
        '''Can track a job and it appears in "track"'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('track', 0, 'track', 'jid')
        self.assertEqual(self.lua('track', 0), {
            'jobs': [{
                'retries': 5,
                'jid': 'jid',
                'tracked': True,
                'tags': {},
                'worker': u'',
                'expires': 0,
                'priority': 0,
                'queue': 'queue',
                'failure': {},
                'state': 'waiting',
                'dependencies': {},
                'klass': 'klass',
                'dependents': {},
                'data': '{}',
                'remaining': 5,
                'history': [{
                    'q': 'queue', 'what': 'put', 'when': 0
                }]
            }], 'expired': {}})

    def test_untrack(self):
        '''We can stop tracking a job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('track', 0, 'track', 'jid')
        self.lua('track', 0, 'untrack', 'jid')
        self.assertEqual(self.lua('track', 0), {'jobs': {}, 'expired': {}})

    def test_track_nonexistent(self):
        '''Tracking nonexistent jobs raises an error'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'track', 0, 'track', 'jid')

    def test_jobs_tracked(self):
        '''Jobs know when they're tracked'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('track', 0, 'track', 'jid')
        self.assertEqual(self.lua('get', 0, 'jid')['tracked'], True)

    def test_jobs_untracked(self):
        '''Jobs know when they're not tracked'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('get', 0, 'jid')['tracked'], False)
