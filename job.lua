-------------------------------------------------------------------------------
-- Job Class
--
-- It returns an object that represents the job with the provided JID
-------------------------------------------------------------------------------

-- This gets all the data associated with the job with the provided id. If the
-- job is not found, it returns nil. If found, it returns an object with the
-- appropriate properties
function QlessJob:data(...)
    local job = redis.call(
            'hmget', QlessJob.ns .. self.jid, 'jid', 'klass', 'state', 'queue',
            'worker', 'priority', 'expires', 'retries', 'remaining', 'data',
            'tags', 'failure')

    -- Return nil if we haven't found it
    if not job[1] then
        return nil
    end

    local data = {
        jid          = job[1],
        klass        = job[2],
        state        = job[3],
        queue        = job[4],
        worker       = job[5] or '',
        tracked      = redis.call(
            'zscore', 'ql:tracked', self.jid) ~= false,
        priority     = tonumber(job[6]),
        expires      = tonumber(job[7]) or 0,
        retries      = tonumber(job[8]),
        remaining    = math.floor(tonumber(job[9])),
        data         = job[10],
        tags         = cjson.decode(job[11]),
        history      = self:history(),
        failure      = cjson.decode(job[12] or '{}'),
        dependents   = redis.call(
            'smembers', QlessJob.ns .. self.jid .. '-dependents'),
        dependencies = redis.call(
            'smembers', QlessJob.ns .. self.jid .. '-dependencies')
    }

    if #arg > 0 then
        -- This section could probably be optimized, but I wanted the interface
        -- in place first
        local response = {}
        for index, key in ipairs(arg) do
            table.insert(response, data[key])
        end
        return response
    else
        return data
    end
end

-- Complete a job and optionally put it in another queue, either scheduled or
-- to be considered waiting immediately. It can also optionally accept other
-- jids on which this job will be considered dependent before it's considered 
-- valid.
--
-- The variable-length arguments may be pairs of the form:
-- 
--      ('next'   , queue) : The queue to advance it to next
--      ('delay'  , delay) : The delay for the next queue
--      ('depends',        : Json of jobs it depends on in the new queue
--          '["jid1", "jid2", ...]')
---
function QlessJob:complete(now, worker, queue, data, ...)
    assert(worker, 'Complete(): Arg "worker" missing')
    assert(queue , 'Complete(): Arg "queue" missing')
    data = assert(cjson.decode(data),
        'Complete(): Arg "data" missing or not JSON: ' .. tostring(data))

    -- Read in all the optional parameters
    local options = {}
    for i = 1, #arg, 2 do options[arg[i]] = arg[i + 1] end
    
    -- Sanity check on optional args
    local nextq   = options['next']
    local delay   = assert(tonumber(options['delay'] or 0))
    local depends = assert(cjson.decode(options['depends'] or '[]'),
        'Complete(): Arg "depends" not JSON: ' .. tostring(options['depends']))

    -- Delay and depends are not allowed together
    if delay > 0 and #depends > 0 then
        error('Complete(): "delay" and "depends" are not allowed together')
    end

    -- Depends doesn't make sense without nextq
    if options['delay'] and nextq == nil then
        error('Complete(): "delay" cannot be used without a "next".')
    end

    -- Depends doesn't make sense without nextq
    if options['depends'] and nextq == nil then
        error('Complete(): "depends" cannot be used without a "next".')
    end

    -- The bin is midnight of the provided day
    -- 24 * 60 * 60 = 86400
    local bin = now - (now % 86400)

    -- First things first, we should see if the worker still owns this job
    local lastworker, state, priority, retries = unpack(
        redis.call('hmget', QlessJob.ns .. self.jid, 'worker', 'state',
            'priority', 'retries', 'dependents'))

    if lastworker == false then
        error('Complete(): Job does not exist')
    elseif (state ~= 'running') then
        error('Complete(): Job is not currently running: ' .. state)
    elseif lastworker ~= worker then
        error('Complete(): Job has been handed out to another worker: ' ..
            tostring(lastworker))
    end

    -- Now we can assume that the worker does own the job. We need to
    --    1) Remove the job from the 'locks' from the old queue
    --    2) Enqueue it in the next stage if necessary
    --    3) Update the data
    --    4) Mark the job as completed, remove the worker, remove expires, and
    --          update history
    self:history(now, 'done')

    if data then
        redis.call('hset', QlessJob.ns .. self.jid, 'data', cjson.encode(data))
    end

    -- Remove the job from the previous queue
    local queue_obj = Qless.queue(queue)
    queue_obj.work.remove(self.jid)
    queue_obj.locks.remove(self.jid)
    queue_obj.scheduled.remove(self.jid)

    ----------------------------------------------------------
    -- This is the massive stats update that we have to do
    ----------------------------------------------------------
    -- This is how long we've been waiting to get popped
    -- local waiting = math.floor(now) - history[#history]['popped']
    local time = tonumber(
        redis.call('hget', QlessJob.ns .. self.jid, 'time') or now)
    local waiting = now - time
    Qless.queue(queue):stat(now, 'run', waiting)
    redis.call('hset', QlessJob.ns .. self.jid,
        'time', string.format("%.20f", now))

    -- Remove this job from the jobs that the worker that was running it has
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

    if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
        Qless.publish('completed', self.jid)
    end

    if nextq then
        queue_obj = Qless.queue(nextq)
        -- Send a message out to log
        Qless.publish('log', cjson.encode({
            jid   = self.jid,
            event = 'advanced',
            queue = queue,
            to    = nextq
        }))

        -- Enqueue the job
        self:history(now, 'put', {q = nextq})

        -- We're going to make sure that this queue is in the
        -- set of known queues
        if redis.call('zscore', 'ql:queues', nextq) == false then
            redis.call('zadd', 'ql:queues', now, nextq)
        end
        
        redis.call('hmset', QlessJob.ns .. self.jid,
            'state', 'waiting',
            'worker', '',
            'failure', '{}',
            'queue', nextq,
            'expires', 0,
            'remaining', tonumber(retries))
        
        if delay > 0 then
            queue_obj.scheduled.add(now + delay, self.jid)
            return 'scheduled'
        else
            -- These are the jids we legitimately have to wait on
            local count = 0
            for i, j in ipairs(depends) do
                -- Make sure it's something other than 'nil' or complete.
                local state = redis.call('hget', QlessJob.ns .. j, 'state')
                if (state and state ~= 'complete') then
                    count = count + 1
                    redis.call(
                        'sadd', QlessJob.ns .. j .. '-dependents',self.jid)
                    redis.call(
                        'sadd', QlessJob.ns .. self.jid .. '-dependencies', j)
                end
            end
            if count > 0 then
                queue_obj.depends.add(now, self.jid)
                redis.call('hset', QlessJob.ns .. self.jid, 'state', 'depends')
                return 'depends'
            else
                queue_obj.work.add(now, priority, self.jid)
                return 'waiting'
            end
        end
    else
        -- Send a message out to log
        Qless.publish('log', cjson.encode({
            jid   = self.jid,
            event = 'completed',
            queue = queue
        }))

        redis.call('hmset', QlessJob.ns .. self.jid,
            'state', 'complete',
            'worker', '',
            'failure', '{}',
            'queue', '',
            'expires', 0,
            'remaining', tonumber(retries))
        
        -- Do the completion dance
        local count = Qless.config.get('jobs-history-count')
        local time  = Qless.config.get('jobs-history')
        
        -- These are the default values
        count = tonumber(count or 50000)
        time  = tonumber(time  or 7 * 24 * 60 * 60)
        
        -- Schedule this job for destructination eventually
        redis.call('zadd', 'ql:completed', now, self.jid)
        
        -- Now look at the expired job data. First, based on the current time
        local jids = redis.call('zrangebyscore', 'ql:completed', 0, now - time)
        -- Any jobs that need to be expired... delete
        for index, jid in ipairs(jids) do
            local tags = cjson.decode(
                redis.call('hget', QlessJob.ns .. jid, 'tags') or '{}')
            for i, tag in ipairs(tags) do
                redis.call('zrem', 'ql:t:' .. tag, jid)
                redis.call('zincrby', 'ql:tags', -1, tag)
            end
            redis.call('del', QlessJob.ns .. jid)
            redis.call('del', QlessJob.ns .. jid .. '-history')
        end
        -- And now remove those from the queued-for-cleanup queue
        redis.call('zremrangebyscore', 'ql:completed', 0, now - time)
        
        -- Now take the all by the most recent 'count' ids
        jids = redis.call('zrange', 'ql:completed', 0, (-1-count))
        for index, jid in ipairs(jids) do
            local tags = cjson.decode(
                redis.call('hget', QlessJob.ns .. jid, 'tags') or '{}')
            for i, tag in ipairs(tags) do
                redis.call('zrem', 'ql:t:' .. tag, jid)
                redis.call('zincrby', 'ql:tags', -1, tag)
            end
            redis.call('del', QlessJob.ns .. jid)
            redis.call('del', QlessJob.ns .. jid .. '-history')
        end
        redis.call('zremrangebyrank', 'ql:completed', 0, (-1-count))
        
        -- Alright, if this has any dependents, then we should go ahead
        -- and unstick those guys.
        for i, j in ipairs(redis.call(
            'smembers', QlessJob.ns .. self.jid .. '-dependents')) do
            redis.call('srem', QlessJob.ns .. j .. '-dependencies', self.jid)
            if redis.call(
                'scard', QlessJob.ns .. j .. '-dependencies') == 0 then
                local q, p = unpack(
                    redis.call('hmget', QlessJob.ns .. j, 'queue', 'priority'))
                if q then
                    local queue = Qless.queue(q)
                    queue.depends.remove(j)
                    queue.work.add(now, p, j)
                    redis.call('hset', QlessJob.ns .. j, 'state', 'waiting')
                end
            end
        end
        
        -- Delete our dependents key
        redis.call('del', QlessJob.ns .. self.jid .. '-dependents')
        
        return 'complete'
    end
end

-- Fail(now, worker, group, message, [data])
-- -------------------------------------------------
-- Mark the particular job as failed, with the provided group, and a more
-- specific message. By `group`, we mean some phrase that might be one of
-- several categorical modes of failure. The `message` is something more
-- job-specific, like perhaps a traceback.
-- 
-- This method should __not__ be used to note that a job has been dropped or
-- has failed in a transient way. This method __should__ be used to note that
-- a job has something really wrong with it that must be remedied.
-- 
-- The motivation behind the `group` is so that similar errors can be grouped
-- together. Optionally, updated data can be provided for the job. A job in
-- any state can be marked as failed. If it has been given to a worker as a 
-- job, then its subsequent requests to heartbeat or complete that job will
-- fail. Failed jobs are kept until they are canceled or completed.
--
-- __Returns__ the id of the failed job if successful, or `False` on failure.
--
-- Args:
--    1) jid
--    2) worker
--    3) group
--    4) message
--    5) the current time
--    6) [data]
function QlessJob:fail(now, worker, group, message, data)
    local worker  = assert(worker           , 'Fail(): Arg "worker" missing')
    local group   = assert(group            , 'Fail(): Arg "group" missing')
    local message = assert(message          , 'Fail(): Arg "message" missing')

    -- The bin is midnight of the provided day
    -- 24 * 60 * 60 = 86400
    local bin = now - (now % 86400)

    if data then
        data = cjson.decode(data)
    end

    -- First things first, we should get the history
    local queue, state = unpack(redis.call('hmget', QlessJob.ns .. self.jid,
        'queue', 'state'))

    -- If the job has been completed, we cannot fail it
    if not state then
        error('Fail(): Job does not exist')
    elseif state ~= 'running' then
        error('Fail(): Job not currently running: ' .. state)
    end

    -- Send out a log message
    Qless.publish('log', cjson.encode({
        jid     = self.jid,
        event   = 'failed',
        worker  = worker,
        group   = group,
        message = message
    }))

    if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
        Qless.publish('failed', self.jid)
    end

    -- Remove this job from the jobs that the worker that was running it has
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

    -- Now, take the element of the history for which our provided worker is
    -- the worker, and update 'failed'
    self:history(now, 'failed', {worker = worker, group = group})

    -- Increment the number of failures for that queue for the
    -- given day.
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failures', 1)
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed'  , 1)

    -- Now remove the instance from the schedule, and work queues for the
    -- queue it's in
    local queue_obj = Qless.queue(queue)
    queue_obj.work.remove(self.jid)
    queue_obj.locks.remove(self.jid)
    queue_obj.scheduled.remove(self.jid)

    -- The reason that this appears here is that the above will fail if the 
    -- job doesn't exist
    if data then
        redis.call('hset', QlessJob.ns .. self.jid, 'data', cjson.encode(data))
    end

    redis.call('hmset', QlessJob.ns .. self.jid,
        'state', 'failed',
        'worker', '',
        'expires', '',
        'failure', cjson.encode({
            ['group']   = group,
            ['message'] = message,
            ['when']    = math.floor(now),
            ['worker']  = worker
        }))

    -- Add this group of failure to the list of failures
    redis.call('sadd', 'ql:failures', group)
    -- And add this particular instance to the failed groups
    redis.call('lpush', 'ql:f:' .. group, self.jid)

    -- Here is where we'd intcrement stats about the particular stage
    -- and possibly the workers

    return self.jid
end

-- retry(now, queue, worker, [delay, [group, [message]]])
-- ------------------------------------------
-- This script accepts jid, queue, worker and delay for retrying a job. This
-- is similar in functionality to `put`, except that this counts against the
-- retries a job has for a stage.
--
-- Throws an exception if:
--      - the worker is not the worker with a lock on the job
--      - the job is not actually running
-- 
-- Otherwise, it returns the number of retries remaining. If the allowed
-- retries have been exhausted, then it is automatically failed, and a negative
-- number is returned.
--
-- If a group and message is provided, then if the retries are exhausted, then
-- the provided group and message will be used in place of the default
-- messaging about retries in the particular queue being exhausted
function QlessJob:retry(now, queue, worker, delay, group, message)
    assert(queue , 'Retry(): Arg "queue" missing')
    assert(worker, 'Retry(): Arg "worker" missing')
    delay = assert(tonumber(delay or 0),
        'Retry(): Arg "delay" not a number: ' .. tostring(delay))
    
    -- Let's see what the old priority, and tags were
    local oldqueue, state, retries, oldworker, priority, failure = unpack(
        redis.call('hmget', QlessJob.ns .. self.jid, 'queue', 'state',
            'retries', 'worker', 'priority', 'failure'))

    -- If this isn't the worker that owns
    if oldworker == false then
        error('Retry(): Job does not exist')
    elseif state ~= 'running' then
        error('Retry(): Job is not currently running: ' .. state)
    elseif oldworker ~= worker then
        error('Retry(): Job has been given to another worker: ' .. oldworker)
    end

    -- For each of these, decrement their retries. If any of them
    -- have exhausted their retries, then we should mark them as
    -- failed.
    local remaining = tonumber(redis.call(
        'hincrby', QlessJob.ns .. self.jid, 'remaining', -1))
    redis.call('hdel', QlessJob.ns .. self.jid, 'grace')

    -- Remove it from the locks key of the old queue
    Qless.queue(oldqueue).locks.remove(self.jid)

    -- Remove this job from the worker that was previously working it
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

    if remaining < 0 then
        -- Now remove the instance from the schedule, and work queues for the
        -- queue it's in
        local group = group or 'failed-retries-' .. queue
        self:history(now, 'failed', {['group'] = group})
        
        redis.call('hmset', QlessJob.ns .. self.jid, 'state', 'failed',
            'worker', '',
            'expires', '')
        -- If the failure has not already been set, then set it
        if group ~= nil and message ~= nil then
            redis.call('hset', QlessJob.ns .. self.jid,
                'failure', cjson.encode({
                    ['group']   = group,
                    ['message'] = message,
                    ['when']    = math.floor(now),
                    ['worker']  = worker
                })
            )
        else
            redis.call('hset', QlessJob.ns .. self.jid,
            'failure', cjson.encode({
                ['group']   = group,
                ['message'] =
                    'Job exhausted retries in queue "' .. oldqueue .. '"',
                ['when']    = now,
                ['worker']  = unpack(self:data('worker'))
            }))
        end
        
        -- Add this type of failure to the list of failures
        redis.call('sadd', 'ql:failures', group)
        -- And add this particular instance to the failed types
        redis.call('lpush', 'ql:f:' .. group, self.jid)
    else
        -- Put it in the queue again with a delay. Like put()
        local queue_obj = Qless.queue(queue)
        if delay > 0 then
            queue_obj.scheduled.add(now + delay, self.jid)
            redis.call('hset', QlessJob.ns .. self.jid, 'state', 'scheduled')
        else
            queue_obj.work.add(now, priority, self.jid)
            redis.call('hset', QlessJob.ns .. self.jid, 'state', 'waiting')
        end

        -- If a group and a message was provided, then we should save it
        if group ~= nil and message ~= nil then
            redis.call('hset', QlessJob.ns .. self.jid,
                'failure', cjson.encode({
                    ['group']   = group,
                    ['message'] = message,
                    ['when']    = math.floor(now),
                    ['worker']  = worker
                })
            )
        end
    end

    return math.floor(remaining)
end

-- Depends(jid, 'on', [jid, [jid, [...]]]
-- Depends(jid, 'off', [jid, [jid, [...]]])
-- Depends(jid, 'off', 'all')
-------------------------------------------------------------------------------
-- Add or remove dependencies a job has. If 'on' is provided, the provided
-- jids are added as dependencies. If 'off' and 'all' are provided, then all
-- the current dependencies are removed. If 'off' is provided and the next
-- argument is not 'all', then those jids are removed as dependencies.
--
-- If a job is not already in the 'depends' state, then this call will return
-- false. Otherwise, it will return true
function QlessJob:depends(now, command, ...)
    assert(command, 'Depends(): Arg "command" missing')
    local state = redis.call('hget', QlessJob.ns .. self.jid, 'state')
    if state ~= 'depends' then
        error('Depends(): Job ' .. self.jid ..
            ' not in the depends state: ' .. tostring(state))
    end

    if command == 'on' then
        -- These are the jids we legitimately have to wait on
        for i, j in ipairs(arg) do
            -- Make sure it's something other than 'nil' or complete.
            local state = redis.call('hget', QlessJob.ns .. j, 'state')
            if (state and state ~= 'complete') then
                redis.call(
                    'sadd', QlessJob.ns .. j .. '-dependents'  , self.jid)
                redis.call(
                    'sadd', QlessJob.ns .. self.jid .. '-dependencies', j)
            end
        end
        return true
    elseif command == 'off' then
        if arg[1] == 'all' then
            for i, j in ipairs(redis.call(
                'smembers', QlessJob.ns .. self.jid .. '-dependencies')) do
                redis.call('srem', QlessJob.ns .. j .. '-dependents', self.jid)
            end
            redis.call('del', QlessJob.ns .. self.jid .. '-dependencies')
            local q, p = unpack(redis.call(
                'hmget', QlessJob.ns .. self.jid, 'queue', 'priority'))
            if q then
                local queue_obj = Qless.queue(q)
                queue_obj.depends.remove(self.jid)
                queue_obj.work.add(now, p, self.jid)
                redis.call('hset', QlessJob.ns .. self.jid, 'state', 'waiting')
            end
        else
            for i, j in ipairs(arg) do
                redis.call('srem', QlessJob.ns .. j .. '-dependents', self.jid)
                redis.call(
                    'srem', QlessJob.ns .. self.jid .. '-dependencies', j)
                if redis.call('scard',
                    QlessJob.ns .. self.jid .. '-dependencies') == 0 then
                    local q, p = unpack(redis.call(
                        'hmget', QlessJob.ns .. self.jid, 'queue', 'priority'))
                    if q then
                        local queue_obj = Qless.queue(q)
                        queue_obj.depends.remove(self.jid)
                        queue_obj.work.add(now, p, self.jid)
                        redis.call('hset',
                            QlessJob.ns .. self.jid, 'state', 'waiting')
                    end
                end
            end
        end
        return true
    else
        error('Depends(): Argument "command" must be "on" or "off"')
    end
end

-- Heartbeat
------------
-- Renew this worker's lock on this job. Throws an exception if:
--      - the job's been given to another worker
--      - the job's been completed
--      - the job's been canceled
--      - the job's not running
function QlessJob:heartbeat(now, worker, data)
    assert(worker, 'Heatbeat(): Arg "worker" missing')

    -- We should find the heartbeat interval for this queue
    -- heartbeat. First, though, we need to find the queue
    -- this particular job is in
    local queue = redis.call('hget', QlessJob.ns .. self.jid, 'queue') or ''
    local expires = now + tonumber(
        Qless.config.get(queue .. '-heartbeat') or
        Qless.config.get('heartbeat', 60))

    if data then
        data = cjson.decode(data)
    end

    -- First, let's see if the worker still owns this job, and there is a
    -- worker
    local job_worker, state = unpack(
        redis.call('hmget', QlessJob.ns .. self.jid, 'worker', 'state'))
    if job_worker == false then
        -- This means the job doesn't exist
        error('Heartbeat(): Job does not exist')
    elseif state ~= 'running' then
        error('Heartbeat(): Job not currently running: ' .. state)
    elseif job_worker ~= worker or #job_worker == 0 then
        error('Heartbeat(): Job given out to another worker: ' .. job_worker)
    else
        -- Otherwise, optionally update the user data, and the heartbeat
        if data then
            -- I don't know if this is wise, but I'm decoding and encoding
            -- the user data to hopefully ensure its sanity
            redis.call('hmset', QlessJob.ns .. self.jid, 'expires',
                expires, 'worker', worker, 'data', cjson.encode(data))
        else
            redis.call('hmset', QlessJob.ns .. self.jid,
                'expires', expires, 'worker', worker)
        end
        
        -- Update hwen this job was last updated on that worker
        -- Add this job to the list of jobs handled by this worker
        redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, self.jid)
        
        -- And now we should just update the locks
        local queue = Qless.queue(
            redis.call('hget', QlessJob.ns .. self.jid, 'queue'))
        queue.locks.add(expires, self.jid)
        return expires
    end
end

-- Priority
-- --------
-- Update the priority of this job. If the job doesn't exist, throws an
-- exception
function QlessJob:priority(priority)
    priority = assert(tonumber(priority),
        'Priority(): Arg "priority" missing or not a number: ' ..
        tostring(priority))

    -- Get the queue the job is currently in, if any
    local queue = redis.call('hget', QlessJob.ns .. self.jid, 'queue')

    if queue == nil then
        -- If the job doesn't exist, throw an error
        error('Priority(): Job ' .. self.jid .. ' does not exist')
    elseif queue == '' then
        -- Just adjust the priority
        redis.call('hset', QlessJob.ns .. self.jid, 'priority', priority)
        return priority
    else
        -- Adjust the priority and see if it's a candidate for updating
        -- its priority in the queue it's currently in
        local queue_obj = Qless.queue(queue)
        if queue_obj.work.score(self.jid) then
            queue_obj.work.add(0, priority, self.jid)
        end
        redis.call('hset', QlessJob.ns .. self.jid, 'priority', priority)
        return priority
    end
end

-- Update the jobs' attributes with the provided dictionary
function QlessJob:update(data)
    local tmp = {}
    for k, v in pairs(data) do
        table.insert(tmp, k)
        table.insert(tmp, v)
    end
    redis.call('hmset', QlessJob.ns .. self.jid, unpack(tmp))
end

-- Times out the job now rather than when its lock is normally set to expire
function QlessJob:timeout(now)
    local queue_name, state, worker = unpack(redis.call('hmget',
        QlessJob.ns .. self.jid, 'queue', 'state', 'worker'))
    if queue_name == nil then
        error('Timeout(): Job does not exist')
    elseif state ~= 'running' then
        error('Timeout(): Job ' .. self.jid .. ' not running')
    else
        -- Time out the job
        self:history(now, 'timed-out')
        local queue = Qless.queue(queue_name)
        queue.locks.remove(self.jid)
        queue.work.add(now, math.huge, self.jid)
        redis.call('hmset', QlessJob.ns .. self.jid,
            'state', 'stalled', 'expires', 0)
        local encoded = cjson.encode({
            jid    = self.jid,
            event  = 'lock_lost',
            worker = worker
        })
        Qless.publish('w:' .. worker, encoded)
        Qless.publish('log', encoded)
        return queue_name
    end
end

-- Return whether or not this job exists
function QlessJob:exists()
    return redis.call('exists', QlessJob.ns .. self.jid) == 1
end

-- Get or append to history
function QlessJob:history(now, what, item)
    -- First, check if there's an old-style history, and update it if there is
    local history = redis.call('hget', QlessJob.ns .. self.jid, 'history')
    if history then
        history = cjson.decode(history)
        for i, value in ipairs(history) do
            redis.call('rpush', QlessJob.ns .. self.jid .. '-history',
                cjson.encode({math.floor(value.put), 'put', {q = value.q}}))

            -- If there's any popped time
            if value.popped then
                redis.call('rpush', QlessJob.ns .. self.jid .. '-history',
                    cjson.encode({math.floor(value.popped), 'popped',
                        {worker = value.worker}}))
            end

            -- If there's any failure
            if value.failed then
                redis.call('rpush', QlessJob.ns .. self.jid .. '-history',
                    cjson.encode(
                        {math.floor(value.failed), 'failed', nil}))
            end

            -- If it was completed
            if value.done then
                redis.call('rpush', QlessJob.ns .. self.jid .. '-history',
                    cjson.encode(
                        {math.floor(value.done), 'done', nil}))
            end
        end
        -- With all this ported forward, delete the old-style history
        redis.call('hdel', QlessJob.ns .. self.jid, 'history')
    end

    -- Now to the meat of the function
    if what == nil then
        -- Get the history
        local response = {}
        for i, value in ipairs(redis.call('lrange',
            QlessJob.ns .. self.jid .. '-history', 0, -1)) do
            value = cjson.decode(value)
            local dict = value[3] or {}
            dict['when'] = value[1]
            dict['what'] = value[2]
            table.insert(response, dict)
        end
        return response
    else
        -- Append to the history
        return redis.call('rpush', QlessJob.ns .. self.jid .. '-history',
            cjson.encode({math.floor(now), what, item}))
    end
end
