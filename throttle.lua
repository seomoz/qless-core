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
    self.locks.add(jid)
    return true
  else
    self.pending.add(jid)
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
  self.locks.remove(jid)
  next_jid = self.pending.pop
  if next_jid and self:acquire(next_jid) then
    queue_obj = Qless.queue(Qless.job(next_jid).queue)
    queue_obj.throttled.remove(next_jid)
    queue_obj.work.add(now, next_jid)
  end
end

function QlessThrottle:available()
  return self.maximum == 0 or self.locks.count() < self.maximum
end
