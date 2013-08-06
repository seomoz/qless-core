-- Get all the attributes of this particular job
function QlessRecurringJob:data()
  local job = redis.call(
    'hmget', 'ql:r:' .. self.jid, 'jid', 'klass', 'state', 'queue',
    'priority', 'interval', 'retries', 'count', 'data', 'tags', 'backlog')
  
  if not job[1] then
    return nil
  end
  
  return {
    jid          = job[1],
    klass        = job[2],
    state        = job[3],
    queue        = job[4],
    priority     = tonumber(job[5]),
    interval     = tonumber(job[6]),
    retries      = tonumber(job[7]),
    count        = tonumber(job[8]),
    data         = job[9],
    tags         = cjson.decode(job[10]),
    backlog      = tonumber(job[11] or 0)
  }
end

-- Update the recurring job data. Key can be:
--      - priority
--      - interval
--      - retries
--      - data
--      - klass
--      - queue
--      - backlog 
function QlessRecurringJob:update(now, ...)
  local options = {}
  -- Make sure that the job exists
  if redis.call('exists', 'ql:r:' .. self.jid) ~= 0 then
    for i = 1, #arg, 2 do
      local key = arg[i]
      local value = arg[i+1]
      assert(value, 'No value provided for ' .. tostring(key))
      if key == 'priority' or key == 'interval' or key == 'retries' then
        value = assert(tonumber(value), 'Recur(): Arg "' .. key .. '" must be a number: ' .. tostring(value))
        -- If the command is 'interval', then we need to update the
        -- time when it should next be scheduled
        if key == 'interval' then
          local queue, interval = unpack(redis.call('hmget', 'ql:r:' .. self.jid, 'queue', 'interval'))
          Qless.queue(queue).recurring.update(
            value - tonumber(interval), self.jid)
        end
        redis.call('hset', 'ql:r:' .. self.jid, key, value)
      elseif key == 'data' then
        assert(cjson.decode(value), 'Recur(): Arg "data" is not JSON-encoded: ' .. tostring(value))
        redis.call('hset', 'ql:r:' .. self.jid, 'data', value)
      elseif key == 'klass' then
        redis.call('hset', 'ql:r:' .. self.jid, 'klass', value)
      elseif key == 'queue' then
        local queue_obj = Qless.queue(
          redis.call('hget', 'ql:r:' .. self.jid, 'queue'))
        local score = queue_obj.recurring.score(self.jid)
        queue_obj.recurring.remove(self.jid)
        Qless.queue(value).recurring.add(score, self.jid)
        redis.call('hset', 'ql:r:' .. self.jid, 'queue', value)
        -- If we don't already know about the queue, learn about it
        if redis.call('zscore', 'ql:queues', value) == false then
          redis.call('zadd', 'ql:queues', now, value)
        end
      elseif key == 'backlog' then
        value = assert(tonumber(value),
          'Recur(): Arg "backlog" not a number: ' .. tostring(value))
        redis.call('hset', 'ql:r:' .. self.jid, 'backlog', value)
      else
        error('Recur(): Unrecognized option "' .. key .. '"')
      end
    end
    return true
  else
    error('Recur(): No recurring job ' .. self.jid)
  end
end

-- Tags this recurring job with the provided tags
function QlessRecurringJob:tag(...)
  local tags = redis.call('hget', 'ql:r:' .. self.jid, 'tags')
  -- If the job has been canceled / deleted, then return false
  if tags then
    -- Decode the json blob, convert to dictionary
    tags = cjson.decode(tags)
    local _tags = {}
    for i,v in ipairs(tags) do _tags[v] = true end
    
    -- Otherwise, add the job to the sorted set with that tags
    for i=1,#arg do if _tags[arg[i]] == nil then table.insert(tags, arg[i]) end end
    
    tags = cjson.encode(tags)
    redis.call('hset', 'ql:r:' .. self.jid, 'tags', tags)
    return tags
  else
    error('Tag(): Job ' .. self.jid .. ' does not exist')
  end
end

-- Removes a tag from the recurring job
function QlessRecurringJob:untag(...)
  -- Get the existing tags
  local tags = redis.call('hget', 'ql:r:' .. self.jid, 'tags')
  -- If the job has been canceled / deleted, then return false
  if tags then
    -- Decode the json blob, convert to dictionary
    tags = cjson.decode(tags)
    local _tags    = {}
    -- Make a hash
    for i,v in ipairs(tags) do _tags[v] = true end
    -- Delete these from the hash
    for i = 1,#arg do _tags[arg[i]] = nil end
    -- Back into a list
    local results = {}
    for i, tag in ipairs(tags) do if _tags[tag] then table.insert(results, tag) end end
    -- json encode them, set, and return
    tags = cjson.encode(results)
    redis.call('hset', 'ql:r:' .. self.jid, 'tags', tags)
    return tags
  else
    error('Untag(): Job ' .. self.jid .. ' does not exist')
  end
end

-- Stop further occurrences of this job
function QlessRecurringJob:unrecur()
  -- First, find out what queue it was attached to
  local queue = redis.call('hget', 'ql:r:' .. self.jid, 'queue')
  if queue then
    -- Now, delete it from the queue it was attached to, and delete the
    -- thing itself
    Qless.queue(queue).recurring.remove(self.jid)
    redis.call('del', 'ql:r:' .. self.jid)
    return true
  else
    return true
  end
end
