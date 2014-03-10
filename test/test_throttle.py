'''Test throttle-centric operations'''

import redis
from common import TestQless

class TestAcquire(TestQless):
  '''Test acquiring of a throttle lock'''
  def test_acquire(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', 0, 'throttle', 'tid')
    self.assertEqual(self.lua('get', 0, 'jid')['throttle'], 'tid')
