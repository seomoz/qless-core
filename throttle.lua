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

-- Release a throttled resource.
function QlessThrottle:release(now, jid)
  self.locks.remove(jid)
end

-- Returns true if the throttle has locks available, false otherwise.
function QlessThrottle:available()
  return self.maximum == 0 or self.locks.length() < self.maximum
end
