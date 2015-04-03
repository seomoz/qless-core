'''Base class for all of our tests'''

import os
import re
import redis
import qless
import unittest


class TestQless(unittest.TestCase):
    '''Base class for all of our tests'''
    @classmethod
    def setUpClass(cls):
        url = os.environ.get('REDIS_URL', 'redis://localhost:6379/')
        cls.redis = redis.Redis.from_url(url)
        cls.lua = qless.QlessRecorder(cls.redis)

    def tearDown(self):
        self.lua.flush()

    def assertMalformed(self, function, examples):
        '''Ensure that all the example inputs to the function are malformed.'''
        for args in examples:
            try:
                # The reason that we're not using assertRaises is that the error
                # message that is produces is unnecessarily vague, and offers no
                # indication of what arguments actually failed to raise the
                # exception
                function(*args)
                self.assertTrue(False, 'Exception not raised for %s(%s)' % (
                    function.__name__, repr(args)))
            except redis.ResponseError:
                self.assertTrue(True)

    def assertRaisesRegexp(self, typ, regex, func, *args, **kwargs):
        '''Python 2.6 doesn't include this method'''
        try:
            func(*args, **kwargs)
            self.assertFalse(True, 'No exception raised')
        except typ as exc:
            self.assertTrue(re.search(regex, str(exc)),
                '%s does not match %s' % (str(exc), regex))
        except Exception as exc:
            self.assertFalse(True,
                '%s raised, expected %s' % (type(exc).__name__, typ.__name__))

    if 'assertIn' not in dir(unittest.TestCase):
        def assertIn(self, member, container, msg=None):
            """Just like self.assertTrue(a in b), but with a nicer default message."""
            from pprint import saferepr
            if member not in container:
                standardMsg = '%s not found in %s' % (saferepr(member),
                                                      saferepr(container))
                self.fail(msg if msg else standardMsg)
