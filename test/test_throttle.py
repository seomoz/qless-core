'''Test throttle-centric operations'''

import redis
import code
from common import TestQless

class TestThrottle(TestQless):
  '''Test setting throttle data'''
  def test_set(self):
    self.lua('throttle.set', 0, 'tid', 5, 0)
    self.assertEqual(self.redis.hmget('ql:th:tid', 'id')[0], 'tid')
    self.assertEqual(self.redis.hmget('ql:th:tid', 'maximum')[0], '5')
    self.assertEqual(self.redis.ttl('ql:th:tid'), None)

  '''Test setting a expiring throttle'''
  def test_set_with_expiration(self):
    self.lua('throttle.set', 0, 'tid', 5, 1000)
    self.assertNotEqual(self.redis.ttl('ql:th:tid'), None)

  '''Test retrieving throttle ttl'''
  def test_retrieve_ttl(self):
    self.lua('throttle.set', 0, 'tid', 5, 1000)
    self.assertEqual(self.lua('throttle.ttl', 1, 'tid'), self.redis.ttl('ql:th:tid'))

  '''Test retrieving throttle data'''
  def test_get(self):
    self.redis.hmset('ql:th:tid', {'id': 'tid', 'maximum' : 5})
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 5})

  '''Test retrieving uninitiailized throttle data'''
  def test_get(self):
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 0})

  '''Test deleting the throttle data'''
  def test_delete(self):
    self.lua('throttle.set', 0, 'tid', 5)
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 5})
    self.lua('throttle.delete', 0, 'tid')
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 0})

class TestAcquire(TestQless):
  '''Test that a job has a default queue throttle'''
  def test_default_queue_throttle(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
    self.assertEqual(self.lua('get', 0, 'jid')['throttles'], ['ql:q:queue'])

  '''Test that job can specify a throttle'''
  def test_specify_throttle(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.assertEqual(self.lua('get', 0, 'jid')['throttles'], ['tid', 'ql:q:queue'])

  '''Test that a job can acquire a throttle'''
  def test_acquire_throttle(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])

  '''Test that acquiring of a throttle lock properly limits the number of jobs'''
  def test_limit_number_of_locks(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 2, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 3, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 4)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), ['jid2', 'jid3', 'jid4'])

class TestRelease(TestQless):
  '''Test that when there are no pending jobs lock is properly released'''
  def test_no_pending_jobs(self):
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])
    self.lua('complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  '''Test that releasing a lock properly another job in the work queue'''
  def test_next_job_is_moved_into_work_qeueue(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 4, 'throttled', 'queue'), ['jid2'])
    self.lua('complete', 5, 'jid1', 'worker', 'queue', {})
    # Lock should be empty until another job is popped
    self.assertEqual(self.lua('throttle.locks', 6, 'tid'), [])
    self.assertEqual(self.lua('jobs', 7, 'throttled', 'queue'), ['jid2'])
    self.lua('pop', 8, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 9, 'tid'), ['jid2'])
    self.assertEqual(self.lua('jobs', 10, 'throttled', 'queue'), [])


  '''Test that cancelling a job properly adds another job in the work queue'''
  def test_on_cancel_next_job_is_moved_into_work_queue(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 4, 'throttled', 'queue'), ['jid2'])
    self.lua('cancel', 5, 'jid1', 'worker', 'queue', {})
    # Lock should be empty until another job is popped
    self.assertEqual(self.lua('throttle.locks', 6, 'tid'), [])
    self.assertEqual(self.lua('jobs', 7, 'throttled', 'queue'), ['jid2'])
    self.lua('pop', 8, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 9, 'tid'), ['jid2'])
    self.assertEqual(self.lua('jobs', 10, 'throttled', 'queue'), [])


  '''Test that when a job completes it properly releases the lock'''
  def test_on_complete_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  '''Test that when a job fails it properly releases the lock'''
  def test_on_failure_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('fail', 0, 'jid', 'worker', 'failed', 'i failed', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries it properly releases the lock
     and goes back into pending'''
  def test_on_retry_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.lua('retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries it is able to reacquire the lock when next popped'''
  def test_on_retry_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.lua('retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries and no pending jobs it acquires the lock again on next pop'''
  def test_on_retry_no_pending_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('retry', 0, 'jid', 'queue', 'worker', 0, 'retry', 'retrying')
    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries and another job is pending, the retrying job acquires the lock'''
  def test_on_retry_with_pending_lock_is_reacquired(self):
    # The retrying job will only re-acquire the lock if nothing is ahead of it in
    # the work queue that requires that lock
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 4, 'throttled', 'queue'), ['jid2'])
    self.lua('retry', 5, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('jobs', 6, 'throttled', 'queue'), ['jid2'])
    self.lua('pop', 7, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 8, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 9, 'throttled', 'queue'), ['jid2'])

class TestDependents(TestQless):
  def test_dependencies_can_acquire_lock_after_dependent_success(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttles', ['tid'])
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttles', ['tid'])

    self.lua('pop', 4, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 5, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 6, 'throttled', 'queue'), [])
    self.lua('complete', 7, 'jid1', 'worker', 'queue', {})

    self.lua('pop', 8, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 9, 'tid'), ['jid2'])
    self.assertEqual(self.lua('jobs', 10, 'throttled', 'queue'), [])
    self.lua('complete', 11, 'jid2', 'worker', 'queue', {})

    self.lua('pop', 12, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 13, 'tid'), ['jid3'])
    self.assertEqual(self.lua('jobs', 14, 'throttled', 'queue'), [])
    self.lua('complete', 15, 'jid3', 'worker', 'queue', {})

    self.assertEqual(self.lua('throttle.locks', 16, 'tid'), [])
    self.assertEqual(self.lua('jobs', 17, 'throttled', 'queue'), [])

  def test_dependencies_can_acquire_lock_after_dependent_failure(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 0, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttles', ['tid'])
    self.lua('put', 0, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttles', ['tid'])

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])
    self.lua('fail', 0, 'jid1', 'worker', 'failed', 'i failed', {})

    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])

  def test_dependencies_do_not_acquire_lock_on_dependent_retry(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttles', ['tid'])
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttles', ['tid'])

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])
    self.lua('retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])
    self.lua('complete', 0, 'jid1', 'worker', 'queue', {})

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid2'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])
    self.lua('complete', 0, 'jid2', 'worker', 'queue', {})

    self.lua('pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid3'])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])
    self.lua('complete', 0, 'jid3', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('jobs', 0, 'throttled', 'queue'), [])


class TestConcurrencyLevelChange(TestQless):
  '''Test that changes to concurrency level are handled dynamically'''
  def test_increasing_concurrency_level_activates_pending_jobs(self):
    '''Activates pending jobs when concurrency level of throttle is increased'''
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])

    self.lua('pop', 4, 'queue', 'worker', 3)
    self.assertEqual(self.lua('throttle.locks', 5, 'tid'), ['jid1'])
    self.assertEqual(self.lua('jobs', 6, 'throttled', 'queue'), ['jid2', 'jid3'])
    self.lua('throttle.set', 7, 'tid', 3)
    self.lua('pop', 8, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 9, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 10, 'throttled', 'queue'), [])

  def test_reducing_concurrency_level_without_pending(self):
    '''Operates at reduced concurrency level after current jobs finish'''
    self.lua('throttle.set', 0, 'tid', 3)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 4, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 5, 'worker', 'queue', 'jid5', 'klass', {}, 0, 'throttles', ['tid'])

    self.lua('pop', 6, 'queue', 'worker', 3)
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 8, 'throttled', 'queue'), [])
    self.lua('throttle.set', 9, 'tid', 1)
    self.lua('pop', 10, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 11, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 12, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('complete', 13, 'jid1', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 14, 'tid'), ['jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 15, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('complete', 16, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 17, 'tid'), ['jid3'])
    self.assertEqual(self.lua('jobs', 18, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('complete', 19, 'jid3', 'worker', 'queue', {})
    self.lua('pop', 20, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 21, 'tid'), ['jid4'])
    self.assertEqual(self.lua('jobs', 22, 'throttled', 'queue'), ['jid5'])
    self.lua('complete', 24, 'jid4', 'worker', 'queue', {})
    self.lua('pop', 23, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 25, 'tid'), ['jid5'])
    self.assertEqual(self.lua('jobs', 26, 'throttled', 'queue'), [])

  def test_reducing_concurrency_level_with_pending(self):
    '''Operates at reduced concurrency level after current jobs finish'''
    self.lua('throttle.set', 0, 'tid', 3)
    self.lua('put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 4, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('put', 5, 'worker', 'queue', 'jid5', 'klass', {}, 0, 'throttles', ['tid'])

    self.lua('pop', 6, 'queue', 'worker', 5)
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 8, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('throttle.set', 9, 'tid', 1)
    self.assertEqual(self.lua('throttle.locks', 10, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 11, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('complete', 12, 'jid1', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 13, 'tid'), ['jid2', 'jid3'])
    self.assertEqual(self.lua('jobs', 14, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('complete', 15, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 16, 'tid'), ['jid3'])
    self.assertEqual(self.lua('jobs', 17, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('complete', 18, 'jid3', 'worker', 'queue', {})
    self.lua('pop', 19, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 20, 'tid'), ['jid4'])
    self.assertEqual(self.lua('jobs', 21, 'throttled', 'queue'), ['jid5'])
    self.lua('complete', 23, 'jid4', 'worker', 'queue', {})
    self.lua('pop', 22, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 24, 'tid'), ['jid5'])
    self.assertEqual(self.lua('jobs', 25, 'throttled', 'queue'), [])
