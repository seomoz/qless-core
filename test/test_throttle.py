'''Test throttle-centric operations'''

import redis
import code
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
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 5})

  '''Test deleting the throttle data'''
  def test_delete(self):
    self.lua('throttle.set', 0, 'tid', 5)
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 5})
    self.lua('throttle.delete', 0, 'tid')
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), None)

class TestAcquire(TestQless):
  '''Test that job can specify a throttle'''
  def test_specify_throttle(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.assertEqual(self.lua('get', 0, 'jid')['throttle'], 'tid')

  '''Test that a job can acquire a throttle'''
  def test_acquire_throttle(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])

  '''Test that acquiring of a throttle lock properly limits the number of jobs'''
  def test_limit_number_of_locks(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 2, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 3, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 4)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), ['jid2', 'jid3', 'jid4'])

class TestRelease(TestQless):
  '''Test that when there are no pending jobs lock is properly released'''
  def test_no_pending_jobs(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  '''Test that releasing a lock properly another job in the work queue'''
  def test_next_job_is_moved_into_work_qeueue(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), ['jid2'])
    self.lua('complete', 0, 'jid1', 'worker', 'queue', {})
    # Lock should be empty until another job is popped
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.lua('pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])


  '''Test that when a job completes it properly releases the lock'''
  def test_on_complete_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  '''Test that when a job fails it properly releases the lock'''
  def test_on_failure_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('fail', 0, 'jid', 'worker', 'failed', 'i failed', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  '''Test that when a job retries it properly releases the lock
     and goes back into pending'''
  def test_on_retry_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.lua('retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  '''Test that when a job retries it is able to reacquire the lock when next popped'''
  def test_on_retry_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.lua('retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  '''Test that when a job retries and no pending jobs it acquires the lock again on next pop'''
  def test_on_retry_no_pending_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('retry', 0, 'jid', 'queue', 'worker', 0, 'retry', 'retrying')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  '''Test that when a job retries and another job is pending, the pending job acquires the lock'''
  def test_on_retry_no_pending_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 7, 'tid'), ['jid2'])
    self.lua('retry', 4, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.pending', 7, 'tid'), [])
    self.lua('pop', 5, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 6, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 7, 'tid'), ['jid1'])

class TestDependents(TestQless):
  def test_dependencies_can_acquire_lock_after_dependent_success(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttle', 'tid')
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttle', 'tid')

    self.lua('pop', 4, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 5, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 6, 'tid'), [])
    self.lua('complete', 7, 'jid1', 'worker', 'queue', {})

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('complete', 0, 'jid2', 'worker', 'queue', {})

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid3'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('complete', 0, 'jid3', 'worker', 'queue', {})

    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  def test_dependencies_can_acquire_lock_after_dependent_failure(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 0, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttle', 'tid')
    self.lua('put', 0, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttle', 'tid')

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('fail', 0, 'jid1', 'worker', 'failed', 'i failed', {})

    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

  def test_dependencies_do_not_acquire_lock_on_dependent_retry(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttle', 'tid')
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttle', 'tid')
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttle', 'tid')

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('complete', 0, 'jid1', 'worker', 'queue', {})

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('complete', 0, 'jid2', 'worker', 'queue', {})

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid3'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])
    self.lua('complete', 0, 'jid3', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), [])

# class TestRecurring(TestQless):
