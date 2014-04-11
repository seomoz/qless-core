-- Retrieve the data fro a throttled resource
function QlessThrottle:data()
  local throttle = redis.call('hmget', QlessThrottle.ns .. self.id, 'id', 'maximum')
  -- Return default if it doesn't exist
  if not throttle[1] then
    return {id = self.id, maximum = 0}
  end

  local data = {
    id = throttle[1],
    maximum = tonumber(throttle[2])
  }
  return data
end

-- Set the data for a throttled resource
function QlessThrottle:set(data, expiration)
  redis.call('hmset', QlessThrottle.ns .. self.id, 'id', self.id, 'maximum', data.maximum)
  if expiration > 0 then
    redis.call('expire', QlessThrottle.ns .. self.id, expiration)
  end
end

-- Delete a throttled resource
function QlessThrottle:unset()
  redis.call('del', QlessThrottle.ns .. self.id)
end

-- Acquire a throttled resource for a job.
-- Returns true of the job acquired the resource, false otherwise
function QlessThrottle:acquire(jid)
  if not self:available() then
    return false
  end

  self.locks.add(1, jid)
  return true
end

function QlessThrottle:pend(now, jid)
  self.pending.add(now, jid)
end

-- Releases the lock taken by the specified jid.
-- number of jobs released back into the queues is determined by the locks_available method.
function QlessThrottle:release(now, jid)
  self.locks.remove(jid)

  local available_locks = self:locks_available()
  if self.pending.length() == 0 or available_locks < 1 then
    return
  end

  -- subtract one to ensure we pop the correct amount. peek(0, 0) returns the first element
  -- peek(0,1) return the first two.
  for _, jid in ipairs(self.pending.peek(0, available_locks - 1)) do
    local job = Qless.job(jid)
    local data = job:data()
    local queue = Qless.queue(data['queue'])

    queue.throttled.remove(jid)
    queue.work.add(now, data.priority, jid)
  end

  -- subtract one to ensure we pop the correct amount. pop(0, 0) pops the first element
  -- pop(0,1) pops the first two.
  local popped = self.pending.pop(0, available_locks - 1)
end

-- Returns true if the throttle has locks available, false otherwise.
function QlessThrottle:available()
  return self.maximum == 0 or self.locks.length() < self.maximum
end

-- Returns the TTL of the throttle
function QlessThrottle:ttl()
  return redis.call('ttl', QlessThrottle.ns .. self.id)
end

-- Returns the number of locks available for the throttle.
-- calculated by maximum - locks.length(), if the throttle is unlimited
-- then up to 10 jobs are released.
function QlessThrottle:locks_available()
  if self.maximum == 0 then
    -- Arbitrarily chosen value. might want to make it configurable in the future.
    return 10
  end

  return self.maximum - self.locks.length()
end
