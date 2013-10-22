-------------------------------------------------------------------------------
-- Queue class
-------------------------------------------------------------------------------
-- Return a queue object
function Qless.queue(name)
  assert(name, 'Queue(): no queue name provided')
  local queue = {}
  setmetatable(queue, QlessQueue)
  queue.name = name

  -- Access to our work
  queue.work = {
    peek = function(count)
      if count == 0 then
        return {}
      end
      local jids = {}
      for index, jid in ipairs(redis.call(
        'zrevrange', queue:prefix('work'), 0, count - 1)) do
        table.insert(jids, jid)
      end
      return jids
    end, remove = function(...)
      if #arg > 0 then
        return redis.call('zrem', queue:prefix('work'), unpack(arg))
      end
    end, add = function(now, priority, jid)
      return redis.call('zadd',
        queue:prefix('work'), priority - (now / 10000000000), jid)
    end, score = function(jid)
      return redis.call('zscore', queue:prefix('work'), jid)
    end, length = function()
      return redis.call('zcard', queue:prefix('work'))
    end
  }

  -- Access to our locks
  queue.locks = {
    expired = function(now, offset, count)
      return redis.call('zrangebyscore',
        queue:prefix('locks'), -math.huge, now, 'LIMIT', offset, count)
    end, peek = function(now, offset, count)
      return redis.call('zrangebyscore', queue:prefix('locks'),
        now, math.huge, 'LIMIT', offset, count)
    end, add = function(expires, jid)
      redis.call('zadd', queue:prefix('locks'), expires, jid)
    end, remove = function(...)
      if #arg > 0 then
        return redis.call('zrem', queue:prefix('locks'), unpack(arg))
      end
    end, running = function(now)
      return redis.call('zcount', queue:prefix('locks'), now, math.huge)
    end, length = function(now)
      -- If a 'now' is provided, we're interested in how many are before
      -- that time
      if now then
        return redis.call('zcount', queue:prefix('locks'), 0, now)
      else
        return redis.call('zcard', queue:prefix('locks'))
      end
    end
  }

  -- Access to our dependent jobs
  queue.depends = {
    peek = function(now, offset, count)
      return redis.call('zrange',
        queue:prefix('depends'), offset, offset + count - 1)
    end, add = function(now, jid)
      redis.call('zadd', queue:prefix('depends'), now, jid)
    end, remove = function(...)
      if #arg > 0 then
        return redis.call('zrem', queue:prefix('depends'), unpack(arg))
      end
    end, length = function()
      return redis.call('zcard', queue:prefix('depends'))
    end
  }

  -- Access to our scheduled jobs
  queue.scheduled = {
    peek = function(now, offset, count)
      return redis.call('zrange',
        queue:prefix('scheduled'), offset, offset + count - 1)
    end, ready = function(now, offset, count)
      return redis.call('zrangebyscore',
        queue:prefix('scheduled'), 0, now, 'LIMIT', offset, count)
    end, add = function(when, jid)
      redis.call('zadd', queue:prefix('scheduled'), when, jid)
    end, remove = function(...)
      if #arg > 0 then
        return redis.call('zrem', queue:prefix('scheduled'), unpack(arg))
      end
    end, length = function()
      return redis.call('zcard', queue:prefix('scheduled'))
    end
  }

  -- Access to our recurring jobs
  queue.recurring = {
    peek = function(now, offset, count)
      return redis.call('zrangebyscore', queue:prefix('recur'),
        0, now, 'LIMIT', offset, count)
    end, ready = function(now, offset, count)
    end, add = function(when, jid)
      redis.call('zadd', queue:prefix('recur'), when, jid)
    end, remove = function(...)
      if #arg > 0 then
        return redis.call('zrem', queue:prefix('recur'), unpack(arg))
      end
    end, update = function(increment, jid)
      redis.call('zincrby', queue:prefix('recur'), increment, jid)
    end, score = function(jid)
      return redis.call('zscore', queue:prefix('recur'), jid)
    end, length = function()
      return redis.call('zcard', queue:prefix('recur'))
    end
  }
  return queue
end

-- Return the prefix for this particular queue
function QlessQueue:prefix(group)
  if group then
    return QlessQueue.ns..self.name..'-'..group
  else
    return QlessQueue.ns..self.name
  end
end

-- Stats(now, date)
-- ---------------------
-- Return the current statistics for a given queue on a given date. The
-- results are returned are a JSON blob:
--
--
--  {
--      # These are unimplemented as of yet
--      'failed': 3,
--      'retries': 5,
--      'wait' : {
--          'total'    : ...,
--          'mean'     : ...,
--          'variance' : ...,
--          'histogram': [
--              ...
--          ]
--      }, 'run': {
--          'total'    : ...,
--          'mean'     : ...,
--          'variance' : ...,
--          'histogram': [
--              ...
--          ]
--      }
--  }
--
-- The histogram's data points are at the second resolution for the first
-- minute, the minute resolution for the first hour, the 15-minute resolution
-- for the first day, the hour resolution for the first 3 days, and then at
-- the day resolution from there on out. The `histogram` key is a list of
-- those values.
function QlessQueue:stats(now, date)
  date = assert(tonumber(date),
    'Stats(): Arg "date" missing or not a number: '.. (date or 'nil'))

  -- The bin is midnight of the provided day
  -- 24 * 60 * 60 = 86400
  local bin = date - (date % 86400)

  -- This a table of all the keys we want to use in order to produce a histogram
  local histokeys = {
    's0','s1','s2','s3','s4','s5','s6','s7','s8','s9','s10','s11','s12','s13','s14','s15','s16','s17','s18','s19','s20','s21','s22','s23','s24','s25','s26','s27','s28','s29','s30','s31','s32','s33','s34','s35','s36','s37','s38','s39','s40','s41','s42','s43','s44','s45','s46','s47','s48','s49','s50','s51','s52','s53','s54','s55','s56','s57','s58','s59',
    'm1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14','m15','m16','m17','m18','m19','m20','m21','m22','m23','m24','m25','m26','m27','m28','m29','m30','m31','m32','m33','m34','m35','m36','m37','m38','m39','m40','m41','m42','m43','m44','m45','m46','m47','m48','m49','m50','m51','m52','m53','m54','m55','m56','m57','m58','m59',
    'h1','h2','h3','h4','h5','h6','h7','h8','h9','h10','h11','h12','h13','h14','h15','h16','h17','h18','h19','h20','h21','h22','h23',
    'd1','d2','d3','d4','d5','d6'
  }

  local mkstats = function(name, bin, queue)
    -- The results we'll be sending back
    local results = {}

    local key = 'ql:s:' .. name .. ':' .. bin .. ':' .. queue
    local count, mean, vk = unpack(redis.call('hmget', key, 'total', 'mean', 'vk'))
    
    count = tonumber(count) or 0
    mean  = tonumber(mean) or 0
    vk    = tonumber(vk)
    
    results.count     = count or 0
    results.mean      = mean  or 0
    results.histogram = {}

    if not count then
      results.std = 0
    else
      if count > 1 then
        results.std = math.sqrt(vk / (count - 1))
      else
        results.std = 0
      end
    end

    local histogram = redis.call('hmget', key, unpack(histokeys))
    for i=1,#histokeys do
      table.insert(results.histogram, tonumber(histogram[i]) or 0)
    end
    return results
  end

  local retries, failed, failures = unpack(redis.call('hmget', 'ql:s:stats:' .. bin .. ':' .. self.name, 'retries', 'failed', 'failures'))
  return {
    retries  = tonumber(retries  or 0),
    failed   = tonumber(failed   or 0),
    failures = tonumber(failures or 0),
    wait     = mkstats('wait', bin, self.name),
    run      = mkstats('run' , bin, self.name)
  }
end

-- Peek
-------
-- Examine the next jobs that would be popped from the queue without actually
-- popping them.
function QlessQueue:peek(now, count)
  count = assert(tonumber(count),
    'Peek(): Arg "count" missing or not a number: ' .. tostring(count))

  -- These are the ids that we're going to return. We'll begin with any jobs
  -- that have lost their locks
  local jids = self.locks.expired(now, 0, count)

  -- If we still need jobs in order to meet demand, then we should
  -- look for all the recurring jobs that need jobs run
  self:check_recurring(now, count - #jids)

  -- Now we've checked __all__ the locks for this queue the could
  -- have expired, and are no more than the number requested. If
  -- we still need values in order to meet the demand, then we 
  -- should check if any scheduled items, and if so, we should 
  -- insert them to ensure correctness when pulling off the next
  -- unit of work.
  self:check_scheduled(now, count - #jids)

  -- With these in place, we can expand this list of jids based on the work
  -- queue itself and the priorities therein
  table.extend(jids, self.work.peek(count - #jids))

  return jids
end

-- Return true if this queue is paused
function QlessQueue:paused()
  return redis.call('sismember', 'ql:paused_queues', self.name) == 1
end

-- Pause this queue
--
-- Note: long term, we have discussed adding a rate-limiting
-- feature to qless-core, which would be more flexible and
-- could be used for pausing (i.e. pause = set the rate to 0).
-- For now, this is far simpler, but we should rewrite this
-- in terms of the rate limiting feature if/when that is added.
function QlessQueue.pause(now, ...)
  redis.call('sadd', 'ql:paused_queues', unpack(arg))
end

-- Unpause this queue
function QlessQueue.unpause(...)
  redis.call('srem', 'ql:paused_queues', unpack(arg))
end

-- Checks for expired locks, scheduled and recurring jobs, returning any
-- jobs that are ready to be processes
function QlessQueue:pop(now, worker, count)
  assert(worker, 'Pop(): Arg "worker" missing')
  count = assert(tonumber(count),
    'Pop(): Arg "count" missing or not a number: ' .. tostring(count))

  -- We should find the heartbeat interval for this queue heartbeat
  local expires = now + tonumber(
    Qless.config.get(self.name .. '-heartbeat') or
    Qless.config.get('heartbeat', 60))

  -- If this queue is paused, then return no jobs
  if self:paused() then
    return {}
  end

  -- Make sure we this worker to the list of seen workers
  redis.call('zadd', 'ql:workers', now, worker)

  -- Check our max concurrency, and limit the count
  local max_concurrency = tonumber(
    Qless.config.get(self.name .. '-max-concurrency', 0))

  if max_concurrency > 0 then
    -- Allow at most max_concurrency - #running
    local allowed = math.max(0, max_concurrency - self.locks.running(now))
    count = math.min(allowed, count)
    if count == 0 then
      return {}
    end
  end

  local jids = self:invalidate_locks(now, count)
  -- Now we've checked __all__ the locks for this queue the could
  -- have expired, and are no more than the number requested.

  -- If we still need jobs in order to meet demand, then we should
  -- look for all the recurring jobs that need jobs run
  self:check_recurring(now, count - #jids)

  -- If we still need values in order to meet the demand, then we 
  -- should check if any scheduled items, and if so, we should 
  -- insert them to ensure correctness when pulling off the next
  -- unit of work.
  self:check_scheduled(now, count - #jids)

  -- With these in place, we can expand this list of jids based on the work
  -- queue itself and the priorities therein
  table.extend(jids, self.work.peek(count - #jids))

  local state
  for index, jid in ipairs(jids) do
    local job = Qless.job(jid)
    state = unpack(job:data('state'))
    job:history(now, 'popped', {worker = worker})

    -- Update the wait time statistics
    local time = tonumber(
      redis.call('hget', QlessJob.ns .. jid, 'time') or now)
    local waiting = now - time
    self:stat(now, 'wait', waiting)
    redis.call('hset', QlessJob.ns .. jid,
      'time', string.format("%.20f", now))
    
    -- Add this job to the list of jobs handled by this worker
    redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, jid)
    
    -- Update the jobs data, and add its locks, and return the job
    job:update({
      worker  = worker,
      expires = expires,
      state   = 'running'
    })
    
    self.locks.add(expires, jid)
    
    local tracked = redis.call('zscore', 'ql:tracked', jid) ~= false
    if tracked then
      Qless.publish('popped', jid)
    end
  end

  -- If we are returning any jobs, then we should remove them from the work
  -- queue
  self.work.remove(unpack(jids))

  return jids
end

-- Update the stats for this queue
function QlessQueue:stat(now, stat, val)
  -- The bin is midnight of the provided day
  local bin = now - (now % 86400)
  local key = 'ql:s:' .. stat .. ':' .. bin .. ':' .. self.name

  -- Get the current data
  local count, mean, vk = unpack(
    redis.call('hmget', key, 'total', 'mean', 'vk'))

  -- If there isn't any data there presently, then we must initialize it
  count = count or 0
  if count == 0 then
    mean  = val
    vk    = 0
    count = 1
  else
    count = count + 1
    local oldmean = mean
    mean  = mean + (val - mean) / count
    vk    = vk + (val - mean) * (val - oldmean)
  end

  -- Now, update the histogram
  -- - `s1`, `s2`, ..., -- second-resolution histogram counts
  -- - `m1`, `m2`, ..., -- minute-resolution
  -- - `h1`, `h2`, ..., -- hour-resolution
  -- - `d1`, `d2`, ..., -- day-resolution
  val = math.floor(val)
  if val < 60 then -- seconds
    redis.call('hincrby', key, 's' .. val, 1)
  elseif val < 3600 then -- minutes
    redis.call('hincrby', key, 'm' .. math.floor(val / 60), 1)
  elseif val < 86400 then -- hours
    redis.call('hincrby', key, 'h' .. math.floor(val / 3600), 1)
  else -- days
    redis.call('hincrby', key, 'd' .. math.floor(val / 86400), 1)
  end     
  redis.call('hmset', key, 'total', count, 'mean', mean, 'vk', vk)
end

-- Put(now, jid, klass, data, delay,
--     [priority, p],
--     [tags, t],
--     [retries, r],
--     [depends, '[...]'])
-- -----------------------
-- Insert a job into the queue with the given priority, tags, delay, klass and
-- data.
function QlessQueue:put(now, worker, jid, klass, raw_data, delay, ...)
  assert(jid  , 'Put(): Arg "jid" missing')
  assert(klass, 'Put(): Arg "klass" missing')
  local data = assert(cjson.decode(raw_data),
    'Put(): Arg "data" missing or not JSON: ' .. tostring(raw_data))
  delay = assert(tonumber(delay),
    'Put(): Arg "delay" not a number: ' .. tostring(delay))

  -- Read in all the optional parameters. All of these must come in pairs, so
  -- if we have an odd number of extra args, raise an error
  if #arg % 2 == 1 then
    error('Odd number of additional args: ' .. tostring(arg))
  end
  local options = {}
  for i = 1, #arg, 2 do options[arg[i]] = arg[i + 1] end

  -- Let's see what the old priority and tags were
  local job = Qless.job(jid)
  local priority, tags, oldqueue, state, failure, retries, oldworker =
    unpack(redis.call('hmget', QlessJob.ns .. jid, 'priority', 'tags',
      'queue', 'state', 'failure', 'retries', 'worker'))

  -- If there are old tags, then we should remove the tags this job has
  if tags then
    Qless.tag(now, 'remove', jid, unpack(cjson.decode(tags)))
  end

  -- Sanity check on optional args
  retries  = assert(tonumber(options['retries']  or retries or 5) ,
    'Put(): Arg "retries" not a number: ' .. tostring(options['retries']))
  tags     = assert(cjson.decode(options['tags'] or tags or '[]' ),
    'Put(): Arg "tags" not JSON'          .. tostring(options['tags']))
  priority = assert(tonumber(options['priority'] or priority or 0),
    'Put(): Arg "priority" not a number'  .. tostring(options['priority']))
  local depends = assert(cjson.decode(options['depends'] or '[]') ,
    'Put(): Arg "depends" not JSON: '     .. tostring(options['depends']))

  -- If the job has old dependencies, determine which dependencies are
  -- in the new dependencies but not in the old ones, and which are in the
  -- old ones but not in the new
  if #depends > 0 then
    -- This makes it easier to check if it's in the new list
    local new = {}
    for _, d in ipairs(depends) do new[d] = 1 end

    -- Now find what's in the original, but not the new
    local original = redis.call(
      'smembers', QlessJob.ns .. jid .. '-dependencies')
    for _, dep in pairs(original) do 
      if new[dep] == nil then
        -- Remove k as a dependency
        redis.call('srem', QlessJob.ns .. dep .. '-dependents'  , jid)
        redis.call('srem', QlessJob.ns .. jid .. '-dependencies', dep)
      end
    end
  end

  -- Send out a log message
  Qless.publish('log', cjson.encode({
    jid   = jid,
    event = 'put',
    queue = self.name
  }))

  -- Update the history to include this new change
  job:history(now, 'put', {q = self.name})

  -- If this item was previously in another queue, then we should remove it from there
  if oldqueue then
    local queue_obj = Qless.queue(oldqueue)
    queue_obj.work.remove(jid)
    queue_obj.locks.remove(jid)
    queue_obj.depends.remove(jid)
    queue_obj.scheduled.remove(jid)
  end

  -- If this had previously been given out to a worker, make sure to remove it
  -- from that worker's jobs
  if oldworker and oldworker ~= '' then
    redis.call('zrem', 'ql:w:' .. oldworker .. ':jobs', jid)
    -- If it's a different worker that's putting this job, send a notification
    -- to the last owner of the job
    if oldworker ~= worker then
      -- We need to inform whatever worker had that job
      local encoded = cjson.encode({
        jid    = jid,
        event  = 'lock_lost',
        worker = oldworker
      })
      Qless.publish('w:' .. oldworker, encoded)
      Qless.publish('log', encoded)
    end
  end

  -- If the job was previously in the 'completed' state, then we should
  -- remove it from being enqueued for destructination
  if state == 'complete' then
    redis.call('zrem', 'ql:completed', jid)
  end

  -- Add this job to the list of jobs tagged with whatever tags were supplied
  for i, tag in ipairs(tags) do
    redis.call('zadd', 'ql:t:' .. tag, now, jid)
    redis.call('zincrby', 'ql:tags', 1, tag)
  end

  -- If we're in the failed state, remove all of our data
  if state == 'failed' then
    failure = cjson.decode(failure)
    -- We need to make this remove it from the failed queues
    redis.call('lrem', 'ql:f:' .. failure.group, 0, jid)
    if redis.call('llen', 'ql:f:' .. failure.group) == 0 then
      redis.call('srem', 'ql:failures', failure.group)
    end
    -- The bin is midnight of the provided day
    -- 24 * 60 * 60 = 86400
    local bin = failure.when - (failure.when % 86400)
    -- We also need to decrement the stats about the queue on
    -- the day that this failure actually happened.
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. self.name, 'failed'  , -1)
  end

  -- First, let's save its data
  redis.call('hmset', QlessJob.ns .. jid,
    'jid'      , jid,
    'klass'    , klass,
    'data'     , raw_data,
    'priority' , priority,
    'tags'     , cjson.encode(tags),
    'state'    , ((delay > 0) and 'scheduled') or 'waiting',
    'worker'   , '',
    'expires'  , 0,
    'queue'    , self.name,
    'retries'  , retries,
    'remaining', retries,
    'time'     , string.format("%.20f", now))

  -- These are the jids we legitimately have to wait on
  for i, j in ipairs(depends) do
    -- Make sure it's something other than 'nil' or complete.
    local state = redis.call('hget', QlessJob.ns .. j, 'state')
    if (state and state ~= 'complete') then
      redis.call('sadd', QlessJob.ns .. j .. '-dependents'  , jid)
      redis.call('sadd', QlessJob.ns .. jid .. '-dependencies', j)
    end
  end

  -- Now, if a delay was provided, and if it's in the future,
  -- then we'll have to schedule it. Otherwise, we're just
  -- going to add it to the work queue.
  if delay > 0 then
    if redis.call('scard', QlessJob.ns .. jid .. '-dependencies') > 0 then
      -- We've already put it in 'depends'. Now, we must just save the data
      -- for when it's scheduled
      self.depends.add(now, jid)
      redis.call('hmset', QlessJob.ns .. jid,
        'state', 'depends',
        'scheduled', now + delay)
    else
      self.scheduled.add(now + delay, jid)
    end
  else
    if redis.call('scard', QlessJob.ns .. jid .. '-dependencies') > 0 then
      self.depends.add(now, jid)
      redis.call('hset', QlessJob.ns .. jid, 'state', 'depends')
    else
      self.work.add(now, priority, jid)
    end
  end

  -- Lastly, we're going to make sure that this item is in the
  -- set of known queues. We should keep this sorted by the 
  -- order in which we saw each of these queues
  if redis.call('zscore', 'ql:queues', self.name) == false then
    redis.call('zadd', 'ql:queues', now, self.name)
  end

  if redis.call('zscore', 'ql:tracked', jid) ~= false then
    Qless.publish('put', jid)
  end

  return jid
end

-- Move `count` jobs out of the failed state and into this queue
function QlessQueue:unfail(now, group, count)
  assert(group, 'Unfail(): Arg "group" missing')
  count = assert(tonumber(count or 25),
    'Unfail(): Arg "count" not a number: ' .. tostring(count))

  -- Get up to that many jobs, and we'll put them in the appropriate queue
  local jids = redis.call('lrange', 'ql:f:' .. group, -count, -1)

  -- And now set each job's state, and put it into the appropriate queue
  local toinsert = {}
  for index, jid in ipairs(jids) do
    local job = Qless.job(jid)
    local data = job:data()
    job:history(now, 'put', {q = self.name})
    redis.call('hmset', QlessJob.ns .. data.jid,
      'state'    , 'waiting',
      'worker'   , '',
      'expires'  , 0,
      'queue'    , self.name,
      'remaining', data.retries or 5)
    self.work.add(now, data.priority, data.jid)
  end

  -- Remove these jobs from the failed state
  redis.call('ltrim', 'ql:f:' .. group, 0, -count - 1)
  if (redis.call('llen', 'ql:f:' .. group) == 0) then
    redis.call('srem', 'ql:failures', group)
  end

  return #jids
end

-- Recur a job of type klass in this queue
function QlessQueue:recur(now, jid, klass, raw_data, spec, ...)
  assert(jid  , 'RecurringJob On(): Arg "jid" missing')
  assert(klass, 'RecurringJob On(): Arg "klass" missing')
  assert(spec , 'RecurringJob On(): Arg "spec" missing')
  local data = assert(cjson.decode(raw_data),
    'RecurringJob On(): Arg "data" not JSON: ' .. tostring(raw_data))

  -- At some point in the future, we may have different types of recurring
  -- jobs, but for the time being, we only have 'interval'-type jobs
  if spec == 'interval' then
    local interval = assert(tonumber(arg[1]),
      'Recur(): Arg "interval" not a number: ' .. tostring(arg[1]))
    local offset   = assert(tonumber(arg[2]),
      'Recur(): Arg "offset" not a number: '   .. tostring(arg[2]))
    if interval <= 0 then
      error('Recur(): Arg "interval" must be greater than or equal to 0')
    end

    -- Read in all the optional parameters. All of these must come in
    -- pairs, so if we have an odd number of extra args, raise an error
    if #arg % 2 == 1 then
      error('Odd number of additional args: ' .. tostring(arg))
    end
    
    -- Read in all the optional parameters
    local options = {}
    for i = 3, #arg, 2 do options[arg[i]] = arg[i + 1] end
    options.tags = assert(cjson.decode(options.tags or '{}'),
      'Recur(): Arg "tags" must be JSON string array: ' .. tostring(
        options.tags))
    options.priority = assert(tonumber(options.priority or 0),
      'Recur(): Arg "priority" not a number: ' .. tostring(
        options.priority))
    options.retries = assert(tonumber(options.retries  or 0),
      'Recur(): Arg "retries" not a number: ' .. tostring(
        options.retries))
    options.backlog = assert(tonumber(options.backlog  or 0),
      'Recur(): Arg "backlog" not a number: ' .. tostring(
        options.backlog))

    local count, old_queue = unpack(redis.call('hmget', 'ql:r:' .. jid, 'count', 'queue'))
    count = count or 0

    -- If it has previously been in another queue, then we should remove 
    -- some information about it
    if old_queue then
      Qless.queue(old_queue).recurring.remove(jid)
    end
    
    -- Do some insertions
    redis.call('hmset', 'ql:r:' .. jid,
      'jid'     , jid,
      'klass'   , klass,
      'data'    , raw_data,
      'priority', options.priority,
      'tags'    , cjson.encode(options.tags or {}),
      'state'   , 'recur',
      'queue'   , self.name,
      'type'    , 'interval',
      -- How many jobs we've spawned from this
      'count'   , count,
      'interval', interval,
      'retries' , options.retries,
      'backlog' , options.backlog)
    -- Now, we should schedule the next run of the job
    self.recurring.add(now + offset, jid)
    
    -- Lastly, we're going to make sure that this item is in the
    -- set of known queues. We should keep this sorted by the 
    -- order in which we saw each of these queues
    if redis.call('zscore', 'ql:queues', self.name) == false then
      redis.call('zadd', 'ql:queues', now, self.name)
    end
    
    return jid
  else
    error('Recur(): schedule type "' .. tostring(spec) .. '" unknown')
  end
end

-- Return the length of the queue
function QlessQueue:length()
  return  self.locks.length() + self.work.length() + self.scheduled.length()
end

-------------------------------------------------------------------------------
-- Housekeeping methods
-------------------------------------------------------------------------------
-- Instantiate any recurring jobs that are ready
function QlessQueue:check_recurring(now, count)
  -- This is how many jobs we've moved so far
  local moved = 0
  -- These are the recurring jobs that need work
  local r = self.recurring.peek(now, 0, count)
  for index, jid in ipairs(r) do
    -- For each of the jids that need jobs scheduled, first
    -- get the last time each of them was run, and then increment
    -- it by its interval. While this time is less than now,
    -- we need to keep putting jobs on the queue
    local klass, data, priority, tags, retries, interval, backlog = unpack(
      redis.call('hmget', 'ql:r:' .. jid, 'klass', 'data', 'priority',
        'tags', 'retries', 'interval', 'backlog'))
    local _tags = cjson.decode(tags)
    local score = math.floor(tonumber(self.recurring.score(jid)))
    interval = tonumber(interval)

    -- If the backlog is set for this job, then see if it's been a long
    -- time since the last pop
    backlog = tonumber(backlog or 0)
    if backlog ~= 0 then
      -- Check how many jobs we could concievably generate
      local num = ((now - score) / interval)
      if num > backlog then
        -- Update the score
        score = score + (
          math.ceil(num - backlog) * interval
        )
      end
    end
    
    -- We're saving this value so that in the history, we can accurately 
    -- reflect when the job would normally have been scheduled
    while (score <= now) and (moved < count) do
      local count = redis.call('hincrby', 'ql:r:' .. jid, 'count', 1)
      moved = moved + 1
      
      -- Add this job to the list of jobs tagged with whatever tags were
      -- supplied
      for i, tag in ipairs(_tags) do
        redis.call('zadd', 'ql:t:' .. tag, now, jid .. '-' .. count)
        redis.call('zincrby', 'ql:tags', 1, tag)
      end
      
      -- First, let's save its data
      local child_jid = jid .. '-' .. count
      redis.call('hmset', QlessJob.ns .. child_jid,
        'jid'      , jid .. '-' .. count,
        'klass'    , klass,
        'data'     , data,
        'priority' , priority,
        'tags'     , tags,
        'state'    , 'waiting',
        'worker'   , '',
        'expires'  , 0,
        'queue'    , self.name,
        'retries'  , retries,
        'remaining', retries,
        'time'     , string.format("%.20f", score))
      Qless.job(child_jid):history(score, 'put', {q = self.name})
      
      -- Now, if a delay was provided, and if it's in the future,
      -- then we'll have to schedule it. Otherwise, we're just
      -- going to add it to the work queue.
      self.work.add(score, priority, jid .. '-' .. count)
      
      score = score + interval
      self.recurring.add(score, jid)
    end
  end
end

-- Check for any jobs that have been scheduled, and shovel them onto
-- the work queue. Returns nothing, but afterwards, up to `count`
-- scheduled jobs will be moved into the work queue
function QlessQueue:check_scheduled(now, count)
  -- zadd is a list of arguments that we'll be able to use to
  -- insert into the work queue
  local scheduled = self.scheduled.ready(now, 0, count)
  for index, jid in ipairs(scheduled) do
    -- With these in hand, we'll have to go out and find the 
    -- priorities of these jobs, and then we'll insert them
    -- into the work queue and then when that's complete, we'll
    -- remove them from the scheduled queue
    local priority = tonumber(
      redis.call('hget', QlessJob.ns .. jid, 'priority') or 0)
    self.work.add(now, priority, jid)
    self.scheduled.remove(jid)

    -- We should also update them to have the state 'waiting'
    -- instead of 'scheduled'
    redis.call('hset', QlessJob.ns .. jid, 'state', 'waiting')
  end
end

-- Check for and invalidate any locks that have been lost. Returns the
-- list of jids that have been invalidated
function QlessQueue:invalidate_locks(now, count)
  local jids = {}
  -- Iterate through all the expired locks and add them to the list
  -- of keys that we'll return
  for index, jid in ipairs(self.locks.expired(now, 0, count)) do
    -- Remove this job from the jobs that the worker that was running it
    -- has
    local worker, failure = unpack(
      redis.call('hmget', QlessJob.ns .. jid, 'worker', 'failure'))
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)

    -- We'll provide a grace period after jobs time out for them to give
    -- some indication of the failure mode. After that time, however, we'll
    -- consider the worker dust in the wind
    local grace_period = tonumber(Qless.config.get('grace-period'))

    -- Whether or not we've already sent a coutesy message
    local courtesy_sent = tonumber(
      redis.call('hget', QlessJob.ns .. jid, 'grace') or 0)

    -- If the remaining value is an odd multiple of 0.5, then we'll assume
    -- that we're just sending the message. Otherwise, it's time to
    -- actually hand out the work to another worker
    local send_message = (courtesy_sent ~= 1)
    local invalidate   = not send_message

    -- If the grace period has been disabled, then we'll do both.
    if grace_period <= 0 then
      send_message = true
      invalidate   = true
    end

    if send_message then
      -- This is where we supply a courtesy message and give the worker
      -- time to provide a failure message
      if redis.call('zscore', 'ql:tracked', jid) ~= false then
        Qless.publish('stalled', jid)
      end
      Qless.job(jid):history(now, 'timed-out')
      redis.call('hset', QlessJob.ns .. jid, 'grace', 1)

      -- Send a message to let the worker know that its lost its lock on
      -- the job
      local encoded = cjson.encode({
        jid    = jid,
        event  = 'lock_lost',
        worker = worker
      })
      Qless.publish('w:' .. worker, encoded)
      Qless.publish('log', encoded)
      self.locks.add(now + grace_period, jid)

      -- If we got any expired locks, then we should increment the
      -- number of retries for this stage for this bin. The bin is
      -- midnight of the provided day
      local bin = now - (now % 86400)
      redis.call('hincrby',
        'ql:s:stats:' .. bin .. ':' .. self.name, 'retries', 1)
    end

    if invalidate then
      -- Unset the grace period attribute so that next time we'll send
      -- the grace period
      redis.call('hdel', QlessJob.ns .. jid, 'grace', 0)

      -- See how many remaining retries the job has
      local remaining = tonumber(redis.call(
        'hincrby', QlessJob.ns .. jid, 'remaining', -1))
      
      -- This is where we actually have to time out the work
      if remaining < 0 then
        -- Now remove the instance from the schedule, and work queues
        -- for the queue it's in
        self.work.remove(jid)
        self.locks.remove(jid)
        self.scheduled.remove(jid)
        
        local group = 'failed-retries-' .. Qless.job(jid):data()['queue']
        local job = Qless.job(jid)
        job:history(now, 'failed', {group = group})
        redis.call('hmset', QlessJob.ns .. jid, 'state', 'failed',
          'worker', '',
          'expires', '')
        -- If the failure has not already been set, then set it
        redis.call('hset', QlessJob.ns .. jid,
        'failure', cjson.encode({
          ['group']   = group,
          ['message'] =
            'Job exhausted retries in queue "' .. self.name .. '"',
          ['when']    = now,
          ['worker']  = unpack(job:data('worker'))
        }))
        
        -- Add this type of failure to the list of failures
        redis.call('sadd', 'ql:failures', group)
        -- And add this particular instance to the failed types
        redis.call('lpush', 'ql:f:' .. group, jid)
        
        if redis.call('zscore', 'ql:tracked', jid) ~= false then
          Qless.publish('failed', jid)
        end
        Qless.publish('log', cjson.encode({
          jid     = jid,
          event   = 'failed',
          group   = group,
          worker  = worker,
          message =
            'Job exhausted retries in queue "' .. self.name .. '"'
        }))

        -- Increment the count of the failed jobs
        local bin = now - (now % 86400)
        redis.call('hincrby',
          'ql:s:stats:' .. bin .. ':' .. self.name, 'failures', 1)
        redis.call('hincrby',
          'ql:s:stats:' .. bin .. ':' .. self.name, 'failed'  , 1)
      else
        table.insert(jids, jid)
      end
    end
  end

  return jids
end

-- Forget the provided queues. As in, remove them from the list of known queues
function QlessQueue.deregister(...)
  redis.call('zrem', Qless.ns .. 'queues', unpack(arg))
end

-- Return information about a particular queue, or all queues
--  [
--      {
--          'name': 'testing',
--          'stalled': 2,
--          'waiting': 5,
--          'running': 5,
--          'scheduled': 10,
--          'depends': 5,
--          'recurring': 0
--      }, {
--          ...
--      }
--  ]
function QlessQueue.counts(now, name)
  if name then
    local queue = Qless.queue(name)
    local stalled = queue.locks.length(now)
    -- Check for any scheduled jobs that need to be moved
    queue:check_scheduled(now, queue.scheduled.length())
    return {
      name      = name,
      waiting   = queue.work.length(),
      stalled   = stalled,
      running   = queue.locks.length() - stalled,
      scheduled = queue.scheduled.length(),
      depends   = queue.depends.length(),
      recurring = queue.recurring.length(),
      paused    = queue:paused()
    }
  else
    local queues = redis.call('zrange', 'ql:queues', 0, -1)
    local response = {}
    for index, qname in ipairs(queues) do
      table.insert(response, QlessQueue.counts(now, qname))
    end
    return response
  end
end
