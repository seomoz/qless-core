'''Test out dependency-related code'''

import redis
from common import TestQless


class TestDependencies(TestQless):
    '''Dependency-related tests'''
    def test_unlock(self):
        '''Dependencies unlock their dependents upon completion'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        # Only 'a' should show up
        self.assertEqual(len(self.lua('pop', 1, 'queue', 'worker', 10)), 1)
        self.lua('complete', 2, 'a', 'worker', 'queue', {})
        # And now 'b' should be available
        self.assertEqual(
            self.lua('pop', 3, 'queue', 'worker', 10)[0]['jid'], 'b')

    def test_unlock_with_delay(self):
        '''Dependencies schedule their dependents upon completion'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 1000, 'depends', ['a'])
        # Only 'a' should show up
        self.assertEqual(len(self.lua('pop', 1, 'queue', 'worker', 10)), 1)
        self.lua('complete', 2, 'a', 'worker', 'queue', {})
        # And now 'b' should be scheduled
        self.assertEqual(self.lua('get', 3, 'b')['state'], 'scheduled')
        # After we wait for the delay, it should be available
        self.assertEqual(len(self.lua('peek', 1000, 'queue', 10)), 1)
        self.assertEqual(self.lua('get', 1001, 'b')['state'], 'waiting')

    def test_unlock_with_delay_satisfied(self):
        '''If deps are satisfied, should be scheduled'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 10, 'depends', ['b'])
        self.assertEqual(self.lua('get', 1, 'a')['state'], 'scheduled')

    def test_complete_depends_with_delay(self):
        '''We should be able to complete a job and specify delay and depends'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.assertEqual(len(self.lua('pop', 2, 'queue', 'worker', 1)), 1)
        self.lua('complete', 3, 'a', 'worker', 'queue', {}, 'next', 'foo',
            'depends', ['b'], 'delay', 10)
        # Now its state should be 'depends'
        self.assertEqual(self.lua('get', 4, 'a')['state'], 'depends')
        # Now pop and complete the job it depends on
        self.lua('pop', 5, 'queue', 'worker', 1)
        self.lua('complete', 6, 'b', 'worker', 'queue', {})
        # Now it should be scheduled
        self.assertEqual(self.lua('get', 7, 'a')['state'], 'scheduled')
        self.assertEqual(len(self.lua('peek', 13, 'foo', 10)), 1)
        self.assertEqual(self.lua('get', 14, 'a')['state'], 'waiting')

    def test_complete_depends(self):
        '''Can also add dependencies upon completion'''
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'a', 'klass', {}, 0)
        # Pop 'b', and then complete it it and make it depend on 'a'
        self.lua('pop', 2, 'queue', 'worker', 1)
        self.lua('complete', 3, 'b', 'worker', 'queue', {},
            'depends', ['a'], 'next', 'queue')
        # Ensure that it shows up everywhere it should
        self.assertEqual(self.lua('get', 4, 'b')['state'], 'depends')
        self.assertEqual(self.lua('get', 5, 'b')['dependencies'], ['a'])
        self.assertEqual(self.lua('get', 6, 'a')['dependents'], ['b'])
        # Only one job should be available
        self.assertEqual(len(self.lua('peek', 7, 'queue', 10)), 1)

    def test_satisfied_dependencies(self):
        '''If dependencies are already complete, it should be available'''
        # First, let's try with a job that has been explicitly completed
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 1)
        self.lua('complete', 2, 'a', 'worker', 'queue', {})
        self.assertEqual(self.lua('get', 3, 'a')['state'], 'complete')
        # Now this job should be readily available
        self.lua('put', 4, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertEqual(self.lua('get', 5, 'b')['state'], 'waiting')
        self.assertEqual(self.lua('peek', 6, 'queue', 10)[0]['jid'], 'b')

    def test_nonexistent_dependencies(self):
        '''If dependencies don't exist, they're assumed completed'''
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertEqual(self.lua('get', 1, 'b')['state'], 'waiting')
        self.assertEqual(self.lua('peek', 2, 'queue', 10)[0]['jid'], 'b')

    def test_cancel_dependency_chain(self):
        '''If an entire dependency chain is cancelled together, it's ok'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.lua('cancel', 2, 'a', 'b')
        self.assertEqual(self.lua('get', 3, 'a'), None)
        self.assertEqual(self.lua('get', 4, 'b'), None)

    def test_cancel_incomplete_chain(self):
        '''Cannot bulk cancel if there are additional dependencies'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.lua('put', 2, 'worker', 'queue', 'c', 'klass', {}, 0, 'depends', ['b'])
        # Now, we'll only cancel part of this chain and see that it fails
        self.assertRaisesRegexp(redis.ResponseError, r'is a dependency',
            self.lua, 'cancel', 3, 'a', 'b')

    def test_cancel_with_missing_jobs(self):
        '''If some jobs are already canceled, it's ok'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.lua('cancel', 2, 'a', 'b', 'c')
        self.assertEqual(self.lua('get', 3, 'a'), None)
        self.assertEqual(self.lua('get', 4, 'b'), None)

    def test_cancel_any_order(self):
        '''Can bulk cancel jobs independent of the order'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.lua('cancel', 2, 'b', 'a')
        self.assertEqual(self.lua('get', 3, 'a'), None)
        self.assertEqual(self.lua('get', 4, 'b'), None)

    def test_multiple_dependency(self):
        '''Unlock a job only after all dependencies have been met'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
        # This job depends on all of the above
        self.lua('put', 20, 'worker', 'queue', 'jid', 'klass', {}, 0, 'depends', jids)
        for jid in jids:
            self.assertEqual(self.lua('get', 30, 'jid')['state'], 'depends')
            self.lua('pop', 30, 'queue', 'worker', 1)
            self.lua('complete', 30, jid, 'worker', 'queue', {})

        # With all of these dependencies finally satisfied, it's available
        self.assertEqual(self.lua('get', 40, 'jid')['state'], 'waiting')

    def test_dependency_chain(self):
        '''Test out successive unlocking of a dependency chain'''
        jids = map(str, range(10))
        self.lua('put', 0, 'worker', 'queue', 0, 'klass', {}, 0)
        for jid, dep in zip(jids[1:], jids[:-1]):
            self.lua(
                'put', jid, 'worker', 'queue', jid, 'klass', {}, 0, 'depends', [dep])
        # Now, we should successively pop jobs and they as we complete them
        # we should get the next
        for jid in jids:
            popped = self.lua('pop', 100, 'queue', 'worker', 10)
            self.assertEqual(len(popped), 1)
            self.assertEqual(popped[0]['jid'], jid)
            self.lua('complete', 100, jid, 'worker', 'queue', {})

    def test_add_dependency(self):
        '''We can add dependencies if it's already in the 'depends' state'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.lua('put', 2, 'worker', 'queue', 'c', 'klass', {}, 0, 'depends', ['a'])
        self.lua('depends', 3, 'c', 'on', 'b')
        self.assertEqual(self.lua('get', 4, 'c')['dependencies'], ['a', 'b'])

    def test_remove_dependency(self):
        '''We can remove dependencies'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
        # This job depends on all of the above
        self.lua('put', 100, 'worker', 'queue', 'jid', 'klass', {}, 0, 'depends', jids)
        # Now, we'll remove dependences one at a time
        for jid in jids:
            self.assertEqual(self.lua('get', 100, 'jid')['state'], 'depends')
            self.lua('depends', 100, 'jid', 'off', jid)
        # With all of these dependencies cancelled, this job should be ready
        self.assertEqual(self.lua('get', 100, 'jid')['state'], 'waiting')

    def test_reput_dependency(self):
        '''When we put a job with new deps, new replaces old'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.lua('put', 2, 'worker', 'queue', 'c', 'klass', {}, 0, 'depends', ['a'])
        self.lua('put', 3, 'worker', 'queue', 'c', 'klass', {}, 0, 'depends', ['b'])
        self.assertEqual(self.lua('get', 4, 'c')['dependencies'], ['b'])
        self.assertEqual(self.lua('get', 5, 'a')['dependents'], {})
        self.assertEqual(self.lua('get', 6, 'b')['dependents'], ['c'])
        # Also, let's make sure that its effective dependencies are changed
        self.lua('pop', 7, 'queue', 'worker', 10)
        self.lua('complete', 8, 'a', 'worker', 'queue', {})
        # We should not see the job unlocked
        self.assertEqual(self.lua('pop', 9, 'queue', 'worker', 10), {})
        self.lua('complete', 10, 'b', 'worker', 'queue', {})
        self.assertEqual(
            self.lua('pop', 9, 'queue', 'worker', 10)[0]['jid'], 'c')

    def test_depends_waiting(self):
        '''Cannot add or remove dependencies if the job is waiting'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'on', 'a')
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'off', 'a')

    def test_depends_scheduled(self):
        '''Cannot add or remove dependencies if the job is scheduled'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'on', 'a')
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'off', 'a')

    def test_depends_nonexistent(self):
        '''Cannot add or remove dependencies if the job doesn't exist'''
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'on', 'a')
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'off', 'a')

    def test_depends_failed(self):
        '''Cannot add or remove dependencies if the job is failed'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'on', 'a')
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'off', 'a')

    def test_depends_running(self):
        '''Cannot add or remove dependencies if the job is running'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'on', 'a')
        self.assertRaisesRegexp(redis.ResponseError, r'in the depends state',
            self.lua, 'depends', 0, 'jid', 'off', 'a')
