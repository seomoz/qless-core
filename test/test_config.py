'''Tests for our configuration'''

from common import TestQless


class TestConfig(TestQless):
    '''Test our config scripts'''
    def test_all(self):
        '''Should be able to access all configurations'''
        self.assertEqual(self.lua('config.get', 0), {
            'application': 'qless',
            'grace-period': 10,
            'heartbeat': 60,
            'histogram-history': 7,
            'jobs-history': 604800,
            'jobs-history-count': 50000,
            'stats-history': 30})

    def test_get(self):
        '''Should be able to get each key individually'''
        for key, value in self.lua('config.get', 0).items():
            self.assertEqual(self.lua('config.get', 0, key), value)

    def test_set_get(self):
        '''If we update a configuration setting, we can get it back'''
        self.lua('config.set', 0, 'foo', 'bar')
        self.assertEqual(self.lua('config.get', 0, 'foo'), 'bar')

    def test_unset_default(self):
        '''If we override a default and then unset it, it should return'''
        default = self.lua('config.get', 0, 'heartbeat')
        self.lua('config.set', 0, 'heartbeat', 100)
        self.assertEqual(self.lua('config.get', 0, 'heartbeat'), 100)
        self.lua('config.unset', 0, 'heartbeat')
        self.assertEqual(self.lua('config.get', 0, 'heartbeat'), default)

    def test_unset(self):
        '''If we set and then unset a setting, it should return to None'''
        self.assertEqual(self.lua('config.get', 0, 'foo'), None)
        self.lua('config.set', 0, 'foo', 5)
        self.assertEqual(self.lua('config.get', 0, 'foo'), 5)
        self.lua('config.unset', 0, 'foo')
        self.assertEqual(self.lua('config.get', 0, 'foo'), None)
