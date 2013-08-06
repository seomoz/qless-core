'''Test our own built-in asserts'''

from common import TestQless


class TestAsserts(TestQless):
    '''Ensure our own assert methods raise the exceptions they're supposed to'''
    def test_assertRaisesRegexp(self):
        '''Make sure that our home-brew assertRaisesRegexp works'''
        def func():
            '''Raises wrong error'''
            self.assertRaisesRegexp(NotImplementedError, 'base 10', int, 'foo')
        self.assertRaises(AssertionError, func)

        def func():
            '''Doesn't match regex'''
            self.assertRaisesRegexp(ValueError, 'sklfjlskjflksjfs', int, 'foo')
        self.assertRaises(AssertionError, func)
        self.assertRaises(ValueError, int, 'foo')

        def func():
            '''Doesn't throw any error'''
            self.assertRaisesRegexp(ValueError, 'base 10', int, 5)
        self.assertRaises(AssertionError, func)
