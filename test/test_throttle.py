'''Test throttle-centric operations'''

import redis
from common import TestQless

class TestThrottle(TestQless):
  '''Test setting throttle data'''
  def test_set(self):
    self.lua('throttle.set', 0, 'tid', 5)
    self.assertEqual(self.redis.hmget('ql:t:tid', 'id')[0], 'tid')
    self.assertEqual(self.redis.hmget('ql:t:tid', 'maximum')[0], '5')

  '''Test retrieving throttle data'''
  def test_get(self):
    self.redis.hmset('ql:t:tid', {'id': 'tid', 'maximum' : 5})
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : '5'})

  '''Test acquiring of a throttle lock'''
  def test_acquire(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.assertEqual(self.lua('get', 0, 'jid')['throttle'], 'tid')
