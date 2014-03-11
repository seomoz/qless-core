-- Retrieve the data fro a throttled resource
function QlessThrottle:data()
  local throttle = redis.call('hmget', QlessThrottle.ns .. self.id, 'id', 'maximum')
  -- Return nil if we haven't found it
  if not throttle[1] then
    return nil
  end

  local data = {
    id = throttle[1],
    maximum = tonumber(throttle[2])
  }
  return data
end

-- Set the data for a throttled resource
function QlessThrottle:set(data)
  redis.call('hmset', QlessThrottle.ns .. self.id, 'id', self.id, 'maximum', data.maximum)
end

-- Delete a throttled resource
function QlessThrottle:unset()
  redis.call('del', QlessThrottle.ns .. self.id)
end

-- Acquire a throttled resource for a job.
-- if the resource is at full capacity then add it to the pending
-- set.
-- Returns true of the job acquired the resource.
function QlessThrottle:acquire(jid)
  if self:available() then
    redis.call('set', 'printline', jid .. ' is acquiring the lock for ' .. self.id)
    self.locks.add(1, jid)
    return true
  else
    redis.call('set', 'printline', jid .. ' failed acquiring the lock for ' .. self.id .. ' marked as pending')
    self.pending.add(1, jid)
    return false
  end
end

-- Release a throttled resource.
-- This will take a currently pending job
-- and attempt to acquire a lock.
-- If it succeeds at acquiring a lock then
-- the job will be moved from the throttled
-- queue into the work queue
function QlessThrottle:release(now, jid)
  redis.call('set', 'printline', jid .. ' is releasing lock on ' .. self.id)
  self.locks.remove(jid)
  redis.call('set', 'printline', 'retrieving next job from pending on ' .. self.id)
  local next_jid = unpack(self:pending_pop(0, 0))
  if next_jid then
    local job = Qless.job(next_jid):data()
    local queue_obj = Qless.queue(job.queue)
    queue_obj.throttled.remove(job.jid)
    queue_obj.work.add(now, job.priority, job.jid)
  end
end

function QlessThrottle:lock_pop(min, max)
  local lock = Qless.throttle(self.id).locks
  local jid = lock.peek(min,max)
  lock.pop(min,max)
  return jid
end

function QlessThrottle:pending_pop(min, max)
  local pending = Qless.throttle(self.id).pending
  local jids = pending.peek(min,max)
  pending.pop(min,max)
  return jids
end

-- Returns true if the throttle has locks available, false otherwise.
function QlessThrottle:available()
  redis.call('set', 'printline', 'available ' .. self.maximum .. ' == 0 or ' .. self.locks.count() .. ' < ' .. self.maximum)
  return self.maximum == 0 or self.locks.count() < self.maximum
end
