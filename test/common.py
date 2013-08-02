'''Base class for all of our tests'''

import redis
import qless
import unittest


class TestQless(unittest.TestCase):
    '''Base class for all of our tests'''
    @classmethod
    def setUpClass(cls):
        cls.lua = qless.QlessRecorder(redis.Redis())

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
