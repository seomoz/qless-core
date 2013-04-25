local Qless = {}

-------------------------------------------------------------------------------
-- Configuration interactions
-------------------------------------------------------------------------------
Qless.config = {}

-- This represents our default configuration settings
Qless.config.defaults = {
    ['application']        = 'qless',
    ['heartbeat']          = 60,
    ['stats-history']      = 30,
    ['histogram-history']  = 7,
    ['jobs-history-count'] = 50000,
    ['jobs-history']       = 604800
}

-- Get one or more of the keys
Qless.config.get = function(key, default)
    if key then
        return redis.call('hget', 'ql:config', key) or
            Qless.config.defaults[key] or default
    else
        -- Inspired by redis-lua https://github.com/nrk/redis-lua/blob/version-2.0/src/redis.lua
        local reply = redis.call('hgetall', 'ql:config')
        for i = 1, #reply, 2 do
            Qless.config.defaults[reply[i]] = reply[i + 1]
        end
        return Qless.config.defaults
    end
end

-- Set a configuration variable
Qless.config.set = function(option, value)
    assert(option, 'config.set(): Arg "option" missing')
    assert(value , 'config.set(): Arg "value" missing')
    -- Send out a log message
    redis.call('publish', 'ql:log', cjson.encode({
        event  = 'config.set',
        option = option
    }))

    redis.call('hset', 'ql:config', option, value)
end

-- Unset a configuration option
Qless.config.unset = function(option)
    assert(option, 'config.unset(): Arg "option" missing')
    -- Send out a log message
    redis.call('publish', 'ql:log', cjson.encode({
        event  = 'config.unset',
        option = option
    }))

    redis.call('hdel', 'ql:config', option)
end

-------------------------------------------------------------------------------
-- Job Class
--
-- It returns an object that represents the job with the provided JID
-------------------------------------------------------------------------------
local QlessJob = {}
QlessJob.__index = QlessJob

-- This gets all the data associated with the job with the provided id. If the
-- job is not found, it returns nil. If found, it returns an object with the
-- appropriate properties
function QlessJob:data()
    local job = redis.call(
        'hmget', 'ql:j:' .. self.jid, 'jid', 'klass', 'state', 'queue',
        'worker', 'priority', 'expires', 'retries', 'remaining', 'data',
        'tags', 'history', 'failure')

    -- Return nil if we haven't found it
    if not job[1] then
        return nil
    end

    return {
        jid          = job[1],
        klass        = job[2],
        state        = job[3],
        queue        = job[4],
        worker       = job[5] or '',
        tracked      = redis.call('zscore', 'ql:tracked', self.jid) ~= false,
        priority     = tonumber(job[6]),
        expires      = tonumber(job[7]) or 0,
        retries      = tonumber(job[8]),
        remaining    = tonumber(job[9]),
        data         = cjson.decode(job[10]),
        tags         = cjson.decode(job[11]),
        history      = cjson.decode(job[12]),
        failure      = cjson.decode(job[13] or '{}'),
        dependents   = redis.call(
            'smembers', 'ql:j:' .. self.jid .. '-dependents'),
        dependencies = redis.call(
            'smembers', 'ql:j:' .. self.jid .. '-dependencies')
    }
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
    local lastworker, history, state, priority, retries = unpack(
        redis.call('hmget', 'ql:j:' .. self.jid, 'worker', 'history', 'state',
            'priority', 'retries', 'dependents'))

    if lastworker ~= worker then
        error('Complete(): Job has been handed out to another worker: ' ..
            lastworker)
    elseif (state ~= 'running') then
        error('Complete(): Job is not currently running: ' .. state)
    end

    -- Now we can assume that the worker does own the job. We need to
    --    1) Remove the job from the 'locks' from the old queue
    --    2) Enqueue it in the next stage if necessary
    --    3) Update the data
    --    4) Mark the job as completed, remove the worker, remove expires, and update history

    -- Unpack the history, and update it
    history = cjson.decode(history)
    history[#history]['done'] = math.floor(now)

    if data then
        redis.call('hset', 'ql:j:' .. self.jid, 'data', cjson.encode(data))
    end

    -- Remove the job from the previous queue
    redis.call('zrem', 'ql:q:' .. queue .. '-work', self.jid)
    redis.call('zrem', 'ql:q:' .. queue .. '-locks', self.jid)
    redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', self.jid)

    ----------------------------------------------------------
    -- This is the massive stats update that we have to do
    ----------------------------------------------------------
    -- This is how long we've been waiting to get popped
    local waiting = math.floor(now) - history[#history]['popped']
    -- Now we'll go through the apparently long and arduous process of update
    local count, mean, vk = unpack(redis.call('hmget', 'ql:s:run:' .. bin .. ':' .. queue, 'total', 'mean', 'vk'))
    count = count or 0
    if count == 0 then
        mean  = waiting
        vk    = 0
        count = 1
    else
        count = count + 1
        local oldmean = mean
        mean  = mean + (waiting - mean) / count
        vk    = vk + (waiting - mean) * (waiting - oldmean)
    end
    -- Now, update the histogram
    -- - `s1`, `s2`, ..., -- second-resolution histogram counts
    -- - `m1`, `m2`, ..., -- minute-resolution
    -- - `h1`, `h2`, ..., -- hour-resolution
    -- - `d1`, `d2`, ..., -- day-resolution
    waiting = math.floor(waiting)
    if waiting < 60 then -- seconds
        redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 's' .. waiting, 1)
    elseif waiting < 3600 then -- minutes
        redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 'm' .. math.floor(waiting / 60), 1)
    elseif waiting < 86400 then -- hours
        redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 'h' .. math.floor(waiting / 3600), 1)
    else -- days
        redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 'd' .. math.floor(waiting / 86400), 1)
    end     
    redis.call('hmset', 'ql:s:run:' .. bin .. ':' .. queue, 'total', count, 'mean', mean, 'vk', vk)
    ----------------------------------------------------------

    -- Remove this job from the jobs that the worker that was running it has
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

    if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
        redis.call('publish', 'completed', self.jid)
    end

    if nextq then
        -- Send a message out to log
        redis.call('publish', 'log', cjson.encode({
            jid   = self.jid,
            event = 'advanced',
            queue = queue,
            to    = nextq
        }))

        -- Enqueue the job
        table.insert(history, {
            q     = nextq,
            put   = math.floor(now)
        })

        -- We're going to make sure that this queue is in the
        -- set of known queues
        if redis.call('zscore', 'ql:queues', nextq) == false then
            redis.call('zadd', 'ql:queues', now, nextq)
        end
        
        redis.call('hmset', 'ql:j:' .. self.jid, 'state', 'waiting', 'worker',
            '', 'failure', '{}', 'queue', nextq, 'expires', 0, 'history',
            cjson.encode(history), 'remaining', tonumber(retries))
        
        if delay > 0 then
            redis.call('zadd', 'ql:q:' .. nextq .. '-scheduled', now + delay,
                self.jid)
            return 'scheduled'
        else
            -- These are the jids we legitimately have to wait on
            local count = 0
            for i, j in ipairs(depends) do
                -- Make sure it's something other than 'nil' or complete.
                local state = redis.call('hget', 'ql:j:' .. j, 'state')
                if (state and state ~= 'complete') then
                    count = count + 1
                    redis.call(
                        'sadd', 'ql:j:' .. j .. '-dependents',self.jid)
                    redis.call(
                        'sadd', 'ql:j:' .. self.jid .. '-dependencies', j)
                end
            end
            if count > 0 then
                redis.call(
                    'zadd', 'ql:q:' .. nextq .. '-depends', now, self.jid)
                redis.call('hset', 'ql:j:' .. self.jid, 'state', 'depends')
                return 'depends'
            else
                redis.call('zadd', 'ql:q:' .. nextq .. '-work', priority - (now / 10000000000), self.jid)
                return 'waiting'
            end
        end
    else
        -- Send a message out to log
        redis.call('publish', 'log', cjson.encode({
            jid   = self.jid,
            event = 'completed',
            queue = queue
        }))

        redis.call('hmset', 'ql:j:' .. self.jid, 'state', 'complete', 'worker', '', 'failure', '{}',
            'queue', '', 'expires', 0, 'history', cjson.encode(history), 'remaining', tonumber(retries))
        
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
            local tags = cjson.decode(redis.call('hget', 'ql:j:' .. jid, 'tags') or '{}')
            for i, tag in ipairs(tags) do
                redis.call('zrem', 'ql:t:' .. tag, jid)
                redis.call('zincrby', 'ql:tags', -1, tag)
            end
            redis.call('del', 'ql:j:' .. jid)
        end
        -- And now remove those from the queued-for-cleanup queue
        redis.call('zremrangebyscore', 'ql:completed', 0, now - time)
        
        -- Now take the all by the most recent 'count' ids
        jids = redis.call('zrange', 'ql:completed', 0, (-1-count))
        for index, jid in ipairs(jids) do
            local tags = cjson.decode(redis.call('hget', 'ql:j:' .. jid, 'tags') or '{}')
            for i, tag in ipairs(tags) do
                redis.call('zrem', 'ql:t:' .. tag, jid)
                redis.call('zincrby', 'ql:tags', -1, tag)
            end
            redis.call('del', 'ql:j:' .. jid)
        end
        redis.call('zremrangebyrank', 'ql:completed', 0, (-1-count))
        
        -- Alright, if this has any dependents, then we should go ahead
        -- and unstick those guys.
        for i, j in ipairs(redis.call('smembers', 'ql:j:' .. self.jid .. '-dependents')) do
            redis.call('srem', 'ql:j:' .. j .. '-dependencies', self.jid)
            if redis.call('scard', 'ql:j:' .. j .. '-dependencies') == 0 then
                local q, p = unpack(redis.call('hmget', 'ql:j:' .. j, 'queue', 'priority'))
                if q then
                    redis.call('zrem', 'ql:q:' .. q .. '-depends', j)
                    redis.call('zadd', 'ql:q:' .. q .. '-work', p, j)
                    redis.call('hset', 'ql:j:' .. j, 'state', 'waiting')
                end
            end
        end
        
        -- Delete our dependents key
        redis.call('del', 'ql:j:' .. self.jid .. '-dependents')
        
        return 'complete'
    end
end

-- Fail(jid, worker, group, message, now, [data])
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
    local history, queue, state = unpack(redis.call('hmget', 'ql:j:' .. self.jid, 'history', 'queue', 'state'))

    -- If the job has been completed, we cannot fail it
    if state ~= 'running' then
        error('Fail(): Job not currently running: ' .. state)
    end

    -- Send out a log message
    redis.call('publish', 'log', cjson.encode({
        jid     = self.jid,
        event   = 'failed',
        worker  = worker,
        group   = group,
        message = message
    }))

    if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
        redis.call('publish', 'failed', self.jid)
    end

    -- Remove this job from the jobs that the worker that was running it has
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

    -- Now, take the element of the history for which our provided worker is the worker, and update 'failed'
    history = cjson.decode(history or '[]')
    if #history > 0 then
        for i=#history,1,-1 do
            if history[i]['worker'] == worker then
                history[i]['failed'] = math.floor(now)
            end
        end
    else
        history = {
            {
                worker = worker,
                failed = math.floor(now)
            }
        }
    end

    -- Increment the number of failures for that queue for the
    -- given day.
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failures', 1)
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed'  , 1)

    -- Now remove the instance from the schedule, and work queues for the queue it's in
    redis.call('zrem', 'ql:q:' .. queue .. '-work', self.jid)
    redis.call('zrem', 'ql:q:' .. queue .. '-locks', self.jid)
    redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', self.jid)

    -- The reason that this appears here is that the above will fail if the job doesn't exist
    if data then
        redis.call('hset', 'ql:j:' .. self.jid, 'data', cjson.encode(data))
    end

    redis.call('hmset', 'ql:j:' .. self.jid, 'state', 'failed', 'worker', '',
        'expires', '', 'history', cjson.encode(history), 'failure', cjson.encode({
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

-- retry(0, now, queue, worker, [delay])
-- ------------------------------------------
-- This script accepts jid, queue, worker and delay for
-- retrying a job. This is similar in functionality to 
-- `put`, except that this counts against the retries 
-- a job has for a stage.
--
-- If the worker is not the worker with a lock on the job,
-- then it returns false. If the job is not actually running,
-- then it returns false. Otherwise, it returns the number
-- of retries remaining. If the allowed retries have been
-- exhausted, then it is automatically failed, and a negative
-- number is returned.
function QlessJob:retry(now, queue, worker, delay)
    assert(queue , 'Retry(): Arg "queue" missing')
    assert(worker, 'Retry(): Arg "worker" missing')
    delay = assert(tonumber(delay or 0),
        'Retry(): Arg "delay" not a number: ' .. tostring(delay))
    
    -- Let's see what the old priority, history and tags were
    local oldqueue, state, retries, oldworker, priority = unpack(redis.call('hmget', 'ql:j:' .. self.jid, 'queue', 'state', 'retries', 'worker', 'priority'))

    -- If this isn't the worker that owns
    if oldworker ~= worker then
        error('Retry(): Job has been handed out to another worker: ' .. oldworker)
    elseif state ~= 'running' then
        error('Retry(): Job is not currently running: ' .. state)
    end

    -- Remove it from the locks key of the old queue
    redis.call('zrem', 'ql:q:' .. oldqueue .. '-locks', self.jid)

    local remaining = redis.call('hincrby', 'ql:j:' .. self.jid, 'remaining', -1)

    -- Remove this job from the worker that was previously working it
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

    if remaining < 0 then
        -- Now remove the instance from the schedule, and work queues for the queue it's in
        local group = 'failed-retries-' .. queue
        -- First things first, we should get the history
        local history = redis.call('hget', 'ql:j:' .. self.jid, 'history')
        -- Now, take the element of the history for which our provided worker is the worker, and update 'failed'
        history = cjson.decode(history or '[]')
        history[#history]['failed'] = now
        
        redis.call('hmset', 'ql:j:' .. self.jid, 'state', 'failed', 'worker', '',
            'expires', '', 'history', cjson.encode(history), 'failure', cjson.encode({
                ['group']   = group,
                ['message'] = 'Job exhausted retries in queue "' .. queue .. '"',
                ['when']    = now,
                ['worker']  = worker
            }))
        
        -- Add this type of failure to the list of failures
        redis.call('sadd', 'ql:failures', group)
        -- And add this particular instance to the failed types
        redis.call('lpush', 'ql:f:' .. group, self.jid)
    else
        -- Put it in the queue again with a delay. Like put()
        if delay > 0 then
            redis.call('zadd', 'ql:q:' .. queue .. '-scheduled', now + delay, self.jid)
            redis.call('hset', 'ql:j:' .. self.jid, 'state', 'scheduled')
        else
            redis.call('zadd', 'ql:q:' .. queue .. '-work', priority - (now / 10000000000), self.jid)
            redis.call('hset', 'ql:j:' .. self.jid, 'state', 'waiting')
        end
    end

    return remaining
end

-- Depends(0, jid,
--      ('on', [jid, [jid, [...]]]) |
--      ('off',
--          ('all' | [jid, [jid, [...]]]))
-------------------------------------------------------------------------------
-- Add or remove dependencies a job has. If 'on' is provided, the provided
-- jids are added as dependencies. If 'off' and 'all' are provided, then all
-- the current dependencies are removed. If 'off' is provided and the next
-- argument is not 'all', then those jids are removed as dependencies.
--
-- If a job is not already in the 'depends' state, then this call will return
-- false. Otherwise, it will return true
--
-- Args:
--    1) jid
function QlessJob:depends(command, ...)
    assert(command, 'Depends(): Arg "command" missing')
    if redis.call('hget', 'ql:j:' .. self.jid, 'state') ~= 'depends' then
        return false
    end

    if command == 'on' then
        -- These are the jids we legitimately have to wait on
        for i, j in ipairs(arg) do
            -- Make sure it's something other than 'nil' or complete.
            local state = redis.call('hget', 'ql:j:' .. j, 'state')
            if (state and state ~= 'complete') then
                redis.call('sadd', 'ql:j:' .. j .. '-dependents'  , self.jid)
                redis.call('sadd', 'ql:j:' .. self.jid .. '-dependencies', j)
            end
        end
        return true
    elseif command == 'off' then
        if arg[1] == 'all' then
            for i, j in ipairs(redis.call('smembers', 'ql:j:' .. self.jid .. '-dependencies')) do
                redis.call('srem', 'ql:j:' .. j .. '-dependents', self.jid)
            end
            redis.call('del', 'ql:j:' .. self.jid .. '-dependencies')
            local q, p = unpack(redis.call('hmget', 'ql:j:' .. self.jid, 'queue', 'priority'))
            if q then
                redis.call('zrem', 'ql:q:' .. q .. '-depends', self.jid)
                redis.call('zadd', 'ql:q:' .. q .. '-work', p, self.jid)
                redis.call('hset', 'ql:j:' .. self.jid, 'state', 'waiting')
            end
        else
            for i, j in ipairs(arg) do
                redis.call('srem', 'ql:j:' .. j .. '-dependents', self.jid)
                redis.call('srem', 'ql:j:' .. self.jid .. '-dependencies', j)
                if redis.call('scard', 'ql:j:' .. self.jid .. '-dependencies') == 0 then
                    local q, p = unpack(redis.call('hmget', 'ql:j:' .. self.jid, 'queue', 'priority'))
                    if q then
                        redis.call('zrem', 'ql:q:' .. q .. '-depends', self.jid)
                        redis.call('zadd', 'ql:q:' .. q .. '-work', p, self.jid)
                        redis.call('hset', 'ql:j:' .. self.jid, 'state', 'waiting')
                    end
                end
            end
        end
        return true
    else
        error('Depends(): Argument "command" must be "on" or "off"')
    end
end

-- This scripts conducts a heartbeat for a job, and returns
-- either the new expiration or False if the lock has been
-- given to another node
--
-- Args:
--    1) now
--    2) worker
--    3) [data]
function QlessJob:heartbeat(now, worker, data)
    assert(worker, 'Heatbeat(): Arg "worker" missing')

    -- We should find the heartbeat interval for this queue
    -- heartbeat. First, though, we need to find the queue
    -- this particular job is in
    local queue     = redis.call('hget', 'ql:j:' .. self.jid, 'queue') or ''
    local expires   = now + tonumber(
        Qless.config.get(queue .. '-heartbeat') or
        Qless.config.get('heartbeat', 60))

    if data then
        data = cjson.decode(data)
    end

    -- First, let's see if the worker still owns this job, and there is a worker
    local job_worker = redis.call('hget', 'ql:j:' .. self.jid, 'worker')
    if job_worker ~= worker or #job_worker == 0 then
        error('Heartbeat(): Job has been handed out to another worker: ' .. job_worker)
    else
        -- Otherwise, optionally update the user data, and the heartbeat
        if data then
            -- I don't know if this is wise, but I'm decoding and encoding
            -- the user data to hopefully ensure its sanity
            redis.call('hmset', 'ql:j:' .. self.jid, 'expires', expires, 'worker', worker, 'data', cjson.encode(data))
        else
            redis.call('hmset', 'ql:j:' .. self.jid, 'expires', expires, 'worker', worker)
        end
        
        -- Update hwen this job was last updated on that worker
        -- Add this job to the list of jobs handled by this worker
        redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, self.jid)
        
        -- And now we should just update the locks
        local queue = redis.call('hget', 'ql:j:' .. self.jid, 'queue')
        redis.call('zadd', 'ql:q:'.. queue .. '-locks', expires, self.jid)
        return expires
    end
end

-- priority(0, jid, priority)
-- --------------------------
-- Accepts a jid, and a new priority for the job. If the job 
-- doesn't exist, then return false. Otherwise, return the 
-- updated priority. If the job is waiting, then the change
-- will be reflected in the order in which it's popped
function QlessJob:priority(priority)
    priority = assert(tonumber(priority),
        'Priority(): Arg "priority" missing or not a number: ' .. tostring(priority))

    -- Get the queue the job is currently in, if any
    local queue = redis.call('hget', 'ql:j:' .. self.jid, 'queue')

    if queue == nil then
        return false
    elseif queue == '' then
        -- Just adjust the priority
        redis.call('hset', 'ql:j:' .. self.jid, 'priority', priority)
        return priority
    else
        -- Adjust the priority and see if it's a candidate for updating
        -- its priority in the queue it's currently in
        if redis.call('zscore', 'ql:q:' .. queue .. '-work', self.jid) then
            redis.call('zadd', 'ql:q:' .. queue .. '-work', priority, self.jid)
        end
        redis.call('hset', 'ql:j:' .. self.jid, 'priority', priority)
        return priority
    end
end

function Qless.job(jid)
    local job = {}
    setmetatable(job, QlessJob)
    job.jid = jid
    return job
end

-------------------------------------------------------------------------------
-- Queue class
-------------------------------------------------------------------------------
local QlessQueue = {}
QlessQueue.__index = QlessQueue

-- Stats(0, queue, date)
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
--
-- Args:
--    1) queue
--    2) time
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
        
        local count, mean, vk = unpack(redis.call('hmget', 'ql:s:' .. name .. ':' .. bin .. ':' .. queue, 'total', 'mean', 'vk'))
        
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

        local histogram = redis.call('hmget', 'ql:s:' .. name .. ':' .. bin .. ':' .. queue, unpack(histokeys))
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

-- Instantiate any recurring jobs that are ready
function QlessQueue:update_recurring_jobs(now, count)
    local key = 'ql:q:' .. self.name
    -- This is how many jobs we've moved so far
    local moved = 0
    -- These are the recurring jobs that need work
    local r = redis.call('zrangebyscore', key .. '-recur', 0, now, 'LIMIT', 0, count)
    for index, jid in ipairs(r) do
        -- For each of the jids that need jobs scheduled, first
        -- get the last time each of them was run, and then increment
        -- it by its interval. While this time is less than now,
        -- we need to keep putting jobs on the queue
        local klass, data, priority, tags, retries, interval = unpack(redis.call('hmget', 'ql:r:' .. jid, 'klass', 'data', 'priority', 'tags', 'retries', 'interval'))
        local _tags = cjson.decode(tags)
        
        -- We're saving this value so that in the history, we can accurately 
        -- reflect when the job would normally have been scheduled
        local score = math.floor(tonumber(redis.call('zscore', key .. '-recur', jid)))
        while (score <= now) and (moved < count) do
            local count = redis.call('hincrby', 'ql:r:' .. jid, 'count', 1)
            moved = moved + 1
            
            -- Add this job to the list of jobs tagged with whatever tags were supplied
            for i, tag in ipairs(_tags) do
                redis.call('zadd', 'ql:t:' .. tag, now, jid .. '-' .. count)
                redis.call('zincrby', 'ql:tags', 1, tag)
            end
            
            -- First, let's save its data
            redis.call('hmset', 'ql:j:' .. jid .. '-' .. count,
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
                'history'  , cjson.encode({{
                    -- The job was essentially put in this queue at this time,
                    -- and not the current time
                    q     = self.name,
                    put   = math.floor(score)
                }}))
            
            -- Now, if a delay was provided, and if it's in the future,
            -- then we'll have to schedule it. Otherwise, we're just
            -- going to add it to the work queue.
            redis.call('zadd', key .. '-work', priority - (score / 10000000000), jid .. '-' .. count)
            
            redis.call('zincrby', key .. '-recur', interval, jid)
            score = score + interval
        end
    end
end

function QlessQueue:check_lost_locks(now, count, execute)
    local key = 'ql:q:' .. self.name
    -- zadd is a list of arguments that we'll be able to use to
    -- insert into the work queue
    local zadd = {}
    local r = redis.call('zrangebyscore', key .. '-scheduled', 0, now, 'LIMIT', 0, count)
    for index, jid in ipairs(r) do
        -- With these in hand, we'll have to go out and find the 
        -- priorities of these jobs, and then we'll insert them
        -- into the work queue and then when that's complete, we'll
        -- remove them from the scheduled queue
        table.insert(zadd, tonumber(redis.call('hget', 'ql:j:' .. jid, 'priority') or 0))
        table.insert(zadd, jid)
        -- We should also update them to have the state 'waiting'
        -- instead of 'scheduled'
        redis.call('hset', 'ql:j:' .. jid, 'state', 'waiting')
    end
    
    if #zadd > 0 then
        -- Now add these to the work list, and then remove them
        -- from the scheduled list
        redis.call('zadd', key .. '-work', unpack(zadd))
        redis.call('zrem', key .. '-scheduled', unpack(r))
    end
    
    -- And now we should get up to the maximum number of requested
    -- work items from the work queue.
    local keys = {}
    for index, jid in ipairs(redis.call('zrevrange', key .. '-work', 0, count - 1)) do
        table.insert(keys, jid)
    end
    return keys
end

-- This script takes the name of the queue and then checks
-- for any expired locks, then inserts any scheduled items
-- that are now valid, and lastly returns any work items 
-- that can be handed over.
--
-- Keys:
--    1) queue name
-- Args:
--    1) the number of items to return
--    2) the current time
function QlessQueue:peek(now, count)
    count = assert(tonumber(count),
        'Peek(): Arg "count" missing or not a number: ' .. (count or 'nil'))
    local key     = 'ql:q:' .. self.name
    local count   = assert(tonumber(count) , 'Peek(): Arg "count" missing or not a number: ' .. (count or 'nil'))

    -- These are the ids that we're going to return
    local keys = {}

    -- Iterate through all the expired locks and add them to the list
    -- of keys that we'll return
    for index, jid in ipairs(redis.call('zrangebyscore', key .. '-locks', 0, now, 'LIMIT', 0, count)) do
        table.insert(keys, jid)
    end

    -- If we still need jobs in order to meet demand, then we should
    -- look for all the recurring jobs that need jobs run
    if #keys < count then
        self:update_recurring_jobs(now, count - #keys)
    end

    -- Now we've checked __all__ the locks for this queue the could
    -- have expired, and are no more than the number requested. If
    -- we still need values in order to meet the demand, then we 
    -- should check if any scheduled items, and if so, we should 
    -- insert them to ensure correctness when pulling off the next
    -- unit of work.
    if #keys < count then
        local locks = self:check_lost_locks(now, count - #keys)
        for k, v in ipairs(locks) do table.insert(keys, v) end
    end

    -- Alright, now the `keys` table is filled with all the job
    -- ids which we'll be returning. Now we need to get the 
    -- metadeata about each of these, update their metadata to
    -- reflect which worker they're on, when the lock expires, 
    -- etc., add them to the locks queue and then we have to 
    -- finally return a list of json blobs

    local response = {}
    for index, jid in ipairs(keys) do
        table.insert(response, Qless.job(jid):data())
    end

    return response
end

-- This script takes the name of the queue and then checks
-- for any expired locks, then inserts any scheduled items
-- that are now valid, and lastly returns any work items 
-- that can be handed over.
--
-- Keys:
--    1) queue name
-- Args:
--    1) worker name
--    2) the number of items to return
--    3) the current time
function QlessQueue:pop(now, worker, count)
    local key     = 'ql:q:' .. self.name
    assert(worker, 'Pop(): Arg "worker" missing')
    count = assert(tonumber(count),
        'Pop(): Arg "count" missing or not a number: ' .. (count or 'nil'))

    -- We should find the heartbeat interval for this queue
    -- heartbeat
    local expires   = now + tonumber(
        Qless.config.get(self.name .. '-heartbeat') or
        Qless.config.get('heartbeat', 60))

    -- The bin is midnight of the provided day
    -- 24 * 60 * 60 = 86400
    local bin = now - (now % 86400)

    -- These are the ids that we're going to return
    local keys = {}

    -- Make sure we this worker to the list of seen workers
    redis.call('zadd', 'ql:workers', now, worker)

    if redis.call('sismember', 'ql:paused_queues', self.name) == 1 then
      return {}
    end

    -- Iterate through all the expired locks and add them to the list
    -- of keys that we'll return
    for index, jid in ipairs(redis.call('zrangebyscore', key .. '-locks', 0, now, 'LIMIT', 0, count)) do
        -- Remove this job from the jobs that the worker that was running it has
        local w = redis.call('hget', 'ql:j:' .. jid, 'worker')
        redis.call('zrem', 'ql:w:' .. w .. ':jobs', jid)

        -- Send a message to let the worker know that its lost its lock on the job
        local encoded = cjson.encode({
            jid    = jid,
            event  = 'lock lost',
            worker = w
        })
        redis.call('publish', w, encoded)
        redis.call('publish', 'log', encoded)
        
        -- For each of these, decrement their retries. If any of them
        -- have exhausted their retries, then we should mark them as
        -- failed.
        if redis.call('hincrby', 'ql:j:' .. jid, 'remaining', -1) < 0 then
            -- Now remove the instance from the schedule, and work queues for the queue it's in
            redis.call('zrem', 'ql:q:' .. self.name .. '-work', jid)
            redis.call('zrem', 'ql:q:' .. self.name .. '-locks', jid)
            redis.call('zrem', 'ql:q:' .. self.name .. '-scheduled', jid)
            
            local group = 'failed-retries-' .. self.name
            -- First things first, we should get the history
            local history = redis.call('hget', 'ql:j:' .. jid, 'history')
            
            -- Now, take the element of the history for which our provided worker is the worker, and update 'failed'
            history = cjson.decode(history or '[]')
            history[#history]['failed'] = now
            
            redis.call('hmset', 'ql:j:' .. jid, 'state', 'failed', 'worker', '',
                'expires', '', 'history', cjson.encode(history), 'failure', cjson.encode({
                    ['group']   = group,
                    ['message'] = 'Job exhausted retries in queue "' .. self.name .. '"',
                    ['when']    = now,
                    ['worker']  = history[#history]['worker']
                }))
            
            -- Add this type of failure to the list of failures
            redis.call('sadd', 'ql:failures', group)
            -- And add this particular instance to the failed types
            redis.call('lpush', 'ql:f:' .. group, jid)
            
            if redis.call('zscore', 'ql:tracked', jid) ~= false then
                redis.call('publish', 'failed', jid)
            end
        else
            table.insert(keys, jid)
            
            if redis.call('zscore', 'ql:tracked', jid) ~= false then
                redis.call('publish', 'stalled', jid)
            end
        end
    end
    -- Now we've checked __all__ the locks for this queue the could
    -- have expired, and are no more than the number requested.

    -- If we got any expired locks, then we should increment the
    -- number of retries for this stage for this bin
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. self.name, 'retries', #keys)

    -- If we still need jobs in order to meet demand, then we should
    -- look for all the recurring jobs that need jobs run
    if #keys < count then
        self:update_recurring_jobs(now, count - #keys)
    end

    -- If we still need values in order to meet the demand, then we 
    -- should check if any scheduled items, and if so, we should 
    -- insert them to ensure correctness when pulling off the next
    -- unit of work.
    if #keys < count then    
        -- zadd is a list of arguments that we'll be able to use to
        -- insert into the work queue
        local zadd = {}
        local r = redis.call('zrangebyscore', key .. '-scheduled', 0, now, 'LIMIT', 0, (count - #keys))
        for index, jid in ipairs(r) do
            -- With these in hand, we'll have to go out and find the 
            -- priorities of these jobs, and then we'll insert them
            -- into the work queue and then when that's complete, we'll
            -- remove them from the scheduled queue
            table.insert(zadd, tonumber(redis.call('hget', 'ql:j:' .. jid, 'priority') or 0))
            table.insert(zadd, jid)
        end
        
        -- Now add these to the work list, and then remove them
        -- from the scheduled list
        if #zadd > 0 then
            redis.call('zadd', key .. '-work', unpack(zadd))
            redis.call('zrem', key .. '-scheduled', unpack(r))
        end
        
        -- And now we should get up to the maximum number of requested
        -- work items from the work queue.
        for index, jid in ipairs(redis.call('zrevrange', key .. '-work', 0, (count - #keys) - 1)) do
            table.insert(keys, jid)
        end
    end

    -- Alright, now the `keys` table is filled with all the job
    -- ids which we'll be returning. Now we need to get the 
    -- metadeata about each of these, update their metadata to
    -- reflect which worker they're on, when the lock expires, 
    -- etc., add them to the locks queue and then we have to 
    -- finally return a list of json blobs

    local response = {}
    local state
    local history
    for index, jid in ipairs(keys) do
        -- First, we should get the state and history of the item
        state, history = unpack(redis.call('hmget', 'ql:j:' .. jid, 'state', 'history'))
        
        history = cjson.decode(history or '{}')
        history[#history]['worker'] = worker
        history[#history]['popped'] = math.floor(now)
        
        ----------------------------------------------------------
        -- This is the massive stats update that we have to do
        ----------------------------------------------------------
        -- This is how long we've been waiting to get popped
        local waiting = math.floor(now) - history[#history]['put']
        -- Now we'll go through the apparently long and arduous process of update
        local count, mean, vk = unpack(redis.call('hmget', 'ql:s:wait:' .. bin .. ':' .. self.name, 'total', 'mean', 'vk'))
        count = count or 0
        if count == 0 then
            mean  = waiting
            vk    = 0
            count = 1
        else
            count = count + 1
            local oldmean = mean
            mean  = mean + (waiting - mean) / count
            vk    = vk + (waiting - mean) * (waiting - oldmean)
        end
        -- Now, update the histogram
        -- - `s1`, `s2`, ..., -- second-resolution histogram counts
        -- - `m1`, `m2`, ..., -- minute-resolution
        -- - `h1`, `h2`, ..., -- hour-resolution
        -- - `d1`, `d2`, ..., -- day-resolution
        waiting = math.floor(waiting)
        if waiting < 60 then -- seconds
            redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. self.name, 's' .. waiting, 1)
        elseif waiting < 3600 then -- minutes
            redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. self.name, 'm' .. math.floor(waiting / 60), 1)
        elseif waiting < 86400 then -- hours
            redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. self.name, 'h' .. math.floor(waiting / 3600), 1)
        else -- days
            redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. self.name, 'd' .. math.floor(waiting / 86400), 1)
        end     
        redis.call('hmset', 'ql:s:wait:' .. bin .. ':' .. self.name, 'total', count, 'mean', mean, 'vk', vk)
        ----------------------------------------------------------
        
        -- Add this job to the list of jobs handled by this worker
        redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, jid)
        
        -- Update the jobs data, and add its locks, and return the job
        redis.call(
            'hmset', 'ql:j:' .. jid, 'worker', worker, 'expires', expires,
            'state', 'running', 'history', cjson.encode(history))
        
        redis.call('zadd', key .. '-locks', expires, jid)
        local job = redis.call(
            'hmget', 'ql:j:' .. jid, 'jid', 'klass', 'state', 'queue', 'worker', 'priority',
            'expires', 'retries', 'remaining', 'data', 'tags', 'history', 'failure')
        
        local tracked = redis.call('zscore', 'ql:tracked', jid) ~= false
        if tracked then
            redis.call('publish', 'popped', jid)
        end
        
        table.insert(response, Qless.job(jid):data())
    end

    if #keys > 0 then
        redis.call('zrem', key .. '-work', unpack(keys))
    end

    return response
end

-- Put(1, jid, klass, data, now, delay, [priority, p], [tags, t], [retries, r], [depends, '[...]'])
-- ----------------------------------------------------------------------------
-- This script takes the name of the queue and then the 
-- info about the work item, and makes sure that it's 
-- enqueued.
--
-- At some point, I'd like to able to provide functionality
-- that enables this to generate a unique ID for this piece
-- of work. As such, client libraries should not expose 
-- setting the id from the user, as this is an implementation
-- detail that's likely to change and users should not grow
-- to depend on it.
--
-- Args:
--    1) jid
--    2) klass
--    3) data
--    4) now
--    5) delay
--    *) [priority, p], [tags, t], [retries, r], [depends, '[...]']
function QlessQueue:put(now, jid, klass, data, delay, ...)
    assert(jid  , 'Put(): Arg "jid" missing')
    assert(klass, 'Put(): Arg "klass" missing')
    data  = assert(cjson.decode(data),
        'Put(): Arg "data" missing or not JSON: ' .. tostring(data))
    delay = assert(tonumber(delay),
        'Put(): Arg "delay" not a number: ' .. tostring(delay))

    -- Read in all the optional parameters
    local options = {}
    for i = 1, #arg, 2 do options[arg[i]] = arg[i + 1] end

    -- Let's see what the old priority, history and tags were
    local history, priority, tags, oldqueue, state, failure, retries, worker = unpack(redis.call('hmget', 'ql:j:' .. jid, 'history', 'priority', 'tags', 'queue', 'state', 'failure', 'retries', 'worker'))

    -- Sanity check on optional args
    retries  = assert(tonumber(options['retries']  or retries or 5) , 'Put(): Arg "retries" not a number: ' .. tostring(options['retries']))
    tags     = assert(cjson.decode(options['tags'] or tags or '[]' ), 'Put(): Arg "tags" not JSON'          .. tostring(options['tags']))
    priority = assert(tonumber(options['priority'] or priority or 0), 'Put(): Arg "priority" not a number'  .. tostring(options['priority']))
    local depends = assert(cjson.decode(options['depends'] or '[]') , 'Put(): Arg "depends" not JSON: '     .. tostring(options['depends']))

    -- Delay and depends are not allowed together
    if delay > 0 and #depends > 0 then
        error('Put(): "delay" and "depends" are not allowed to be used together')
    end

    -- Send out a log message
    redis.call('publish', 'log', cjson.encode({
        jid   = jid,
        event = 'put',
        queue = self.name
    }))

    -- Update the history to include this new change
    local history = cjson.decode(history or '{}')
    table.insert(history, {
        q     = self.name,
        put   = math.floor(now)
    })

    -- If this item was previously in another queue, then we should remove it from there
    if oldqueue then
        redis.call('zrem', 'ql:q:' .. oldqueue .. '-work', jid)
        redis.call('zrem', 'ql:q:' .. oldqueue .. '-locks', jid)
        redis.call('zrem', 'ql:q:' .. oldqueue .. '-scheduled', jid)
        redis.call('zrem', 'ql:q:' .. oldqueue .. '-depends', jid)
    end

    -- If this had previously been given out to a worker,
    -- make sure to remove it from that worker's jobs
    if worker then
        redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)
        -- We need to inform whatever worker had that job
        redis.call('publish', worker, cjson.encode({
            jid   = jid,
            event = 'put',
            queue = self.name
        }))
    end

    -- If the job was previously in the 'completed' state, then we should remove
    -- it from being enqueued for destructination
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
    redis.call('hmset', 'ql:j:' .. jid,
        'jid'      , jid,
        'klass'    , klass,
        'data'     , cjson.encode(data),
        'priority' , priority,
        'tags'     , cjson.encode(tags),
        'state'    , ((delay > 0) and 'scheduled') or 'waiting',
        'worker'   , '',
        'expires'  , 0,
        'queue'    , self.name,
        'retries'  , retries,
        'remaining', retries,
        'history'  , cjson.encode(history))

    -- These are the jids we legitimately have to wait on
    for i, j in ipairs(depends) do
        -- Make sure it's something other than 'nil' or complete.
        local state = redis.call('hget', 'ql:j:' .. j, 'state')
        if (state and state ~= 'complete') then
            redis.call('sadd', 'ql:j:' .. j .. '-dependents'  , jid)
            redis.call('sadd', 'ql:j:' .. jid .. '-dependencies', j)
        end
    end

    -- Now, if a delay was provided, and if it's in the future,
    -- then we'll have to schedule it. Otherwise, we're just
    -- going to add it to the work queue.
    if delay > 0 then
        redis.call('zadd', 'ql:q:' .. self.name .. '-scheduled', now + delay, jid)
    else
        if redis.call('scard', 'ql:j:' .. jid .. '-dependencies') > 0 then
            redis.call('zadd', 'ql:q:' .. self.name .. '-depends', now, jid)
            redis.call('hset', 'ql:j:' .. jid, 'state', 'depends')
        else
            redis.call('zadd', 'ql:q:' .. self.name .. '-work', priority - (now / 10000000000), jid)
        end
    end

    -- Lastly, we're going to make sure that this item is in the
    -- set of known queues. We should keep this sorted by the 
    -- order in which we saw each of these queues
    if redis.call('zscore', 'ql:queues', self.name) == false then
        redis.call('zadd', 'ql:queues', now, self.name)
    end

    if redis.call('zscore', 'ql:tracked', jid) ~= false then
        redis.call('publish', 'put', jid)
    end

    return jid
end

function Qless.queue(name)
    local queue = {}
    setmetatable(queue, QlessQueue)
    queue.name = name
    return queue
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
function Qless.queues(now, queue)
    if queue then
        local stalled = redis.call(
            'zcount', 'ql:q:' .. queue .. '-locks', 0, now)
        return {
            name      = queue,
            waiting   = redis.call('zcard', 'ql:q:' .. queue .. '-work'),
            stalled   = stalled,
            running   = 
                redis.call('zcard', 'ql:q:' .. queue .. '-locks') - stalled,
            scheduled = redis.call('zcard', 'ql:q:' .. queue .. '-scheduled'),
            depends   = redis.call('zcard', 'ql:q:' .. queue .. '-depends'),
            recurring = redis.call('zcard', 'ql:q:' .. queue .. '-recur')
        }
    else
        local queues = redis.call('zrange', 'ql:queues', 0, -1)
        local response = {}
        for index, qname in ipairs(queues) do
            local stalled = redis.call(
                'zcount', 'ql:q:' .. qname .. '-locks', 0, now)
            table.insert(response, {
                name      = qname,
                waiting   = redis.call('zcard', 'ql:q:' .. qname .. '-work'),
                stalled   = stalled,
                running   = redis.call(
                    'zcard', 'ql:q:' .. qname .. '-locks') - stalled,
                scheduled = redis.call(
                    'zcard', 'ql:q:' .. qname .. '-scheduled'),
                depends   = redis.call(
                    'zcard', 'ql:q:' .. qname .. '-depends'),
                recurring = redis.call('zcard', 'ql:q:' .. qname .. '-recur')
            })
        end
        return response
    end
end

-- Failed([group, [start, [limit]]])
-- ------------------------------------
-- If no group is provided, this returns a JSON blob of the counts of the
-- various groups of failures known. If a group is provided, it will report up
-- to `limit` from `start` of the jobs affected by that issue.
-- 
--  # If no group, then...
--  {
--      'group1': 1,
--      'group2': 5,
--      ...
--  }
--  
--  # If a group is provided, then...
--  {
--      'total': 20,
--      'jobs': [
--          {
--              # All the normal keys for a job
--              'jid': ...,
--              'data': ...
--              # The message for this particular instance
--              'message': ...,
--              'group': ...,
--          }, ...
--      ]
--  }
--
function Qless.failed(group, start, limit)
    start = assert(tonumber(start or 0),
        'Failed(): Arg "start" is not a number: ' .. (start or 'nil'))
    limit = assert(tonumber(limit or 25),
        'Failed(): Arg "limit" is not a number: ' .. (limit or 'nil'))

    if group then
        -- If a group was provided, then we should do paginated lookup
        local response = {
            total = redis.call('llen', 'ql:f:' .. group),
            jobs  = {}
        }
        local jids = redis.call('lrange', 'ql:f:' .. group, start, limit - 1)
        for index, jid in ipairs(jids) do
            table.insert(response.jobs, Qless.job(jid):data())
        end
        return response
    else
        -- Otherwise, we should just list all the known failure groups we have
        local response = {}
        local groups = redis.call('smembers', 'ql:failures')
        for index, group in ipairs(groups) do
            response[group] = redis.call('llen', 'ql:f:' .. group)
        end
        return response
    end
end

-- Jobs(0, now, 'complete' | (
--      (
--          'stalled' | 'running' | 'scheduled' | 'depends', 'recurring'
--      ), queue)
-- [offset, [count]])
-------------------------------------------------------------------------------
-- 
-- Return all the job ids currently considered to be in the provided state
-- in a particular queue. The response is a list of job ids:
-- 
--  [
--      jid1, 
--      jid2,
--      ...
--  ]
function Qless.jobs(now, state, ...)
    assert(state, 'Jobs(): Arg "state" missing')
    if state == 'complete' then
        local offset = assert(tonumber(arg[1] or 0),
            'Jobs(): Arg "offset" not a number: ' .. tostring(arg[1]))
        local count  = assert(tonumber(arg[2] or 25),
            'Jobs(): Arg "count" not a number: ' .. tostring(arg[2]))
        return redis.call('zrevrange', 'ql:completed', offset,
            offset + count - 1)
    else
        local queue  = assert(arg[1], 'Jobs(): Arg "queue" missing')
        local offset = assert(tonumber(arg[2] or 0),
            'Jobs(): Arg "offset" not a number: ' .. tostring(arg[2]))
        local count  = assert(tonumber(arg[3] or 25),
            'Jobs(): Arg "count" not a number: ' .. tostring(arg[3]))

        if state == 'running' then
            return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', now, 133389432700, 'limit', offset, count)
        elseif state == 'stalled' then
            return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', 0, now, 'limit', offset, count)
        elseif state == 'scheduled' then
            return redis.call('zrange', 'ql:q:' .. queue .. '-scheduled', offset, offset + count - 1)
        elseif state == 'depends' then
            return redis.call('zrange', 'ql:q:' .. queue .. '-depends', offset, offset + count - 1)
        elseif state == 'recurring' then
            return redis.call('zrange', 'ql:q:' .. queue .. '-recur', offset, offset + count - 1)
        else
            error('Jobs(): Unknown type "' .. state .. '"')
        end
    end
end

-- Workers(0, now, [worker])
----------------------------
-- Provide data about all the workers, or if a specific worker is provided,
-- then which jobs that worker is responsible for. If no worker is provided,
-- expect a response of the form:
-- 
--  [
--      # This is sorted by the recency of activity from that worker
--      {
--          'name'   : 'hostname1-pid1',
--          'jobs'   : 20,
--          'stalled': 0
--      }, {
--          ...
--      }
--  ]
-- 
-- If a worker id is provided, then expect a response of the form:
-- 
--  {
--      'jobs': [
--          jid1,
--          jid2,
--          ...
--      ], 'stalled': [
--          jid1,
--          ...
--      ]
--  }
--
function Qless.workers(now, worker)
    -- Clean up all the workers' job lists if they're too old. This is
    -- determined by the `max-worker-age` configuration, defaulting to the
    -- last day. Seems like a 'reasonable' default
    local interval = tonumber(Qless.config.get('max-worker-age', 86400))

    local workers  = redis.call('zrangebyscore', 'ql:workers', 0, now - interval)
    for index, worker in ipairs(workers) do
        redis.call('del', 'ql:w:' .. worker .. ':jobs')
    end

    -- And now remove them from the list of known workers
    redis.call('zremrangebyscore', 'ql:workers', 0, now - interval)

    if worker then
        return {
            jobs    = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now + 8640000, now),
            stalled = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now, 0)
        }
    else
        local response = {}
        local workers = redis.call('zrevrange', 'ql:workers', 0, -1)
        for index, worker in ipairs(workers) do
            table.insert(response, {
                name    = worker,
                jobs    = redis.call('zcount', 'ql:w:' .. worker .. ':jobs', now, now + 8640000),
                stalled = redis.call('zcount', 'ql:w:' .. worker .. ':jobs', 0, now)
            })
        end
        return response
    end
end

-- Track(0)
-- Track(0, ('track' | 'untrack'), jid, now)
-- ------------------------------------------
-- If no arguments are provided, it returns details of all currently-tracked
-- jobs. If the first argument is 'track', then it will start tracking the job
-- associated with that id, and 'untrack' stops tracking it. In this context,
-- tracking is nothing more than saving the job to a list of jobs that are
-- considered special.
-- 
--  {
--      'jobs': [
--          {
--              'jid': ...,
--              # All the other details you'd get from 'get'
--          }, {
--              ...
--          }
--      ], 'expired': [
--          # These are all the jids that are completed and whose data expired
--          'deadbeef',
--          ...,
--          ...,
--      ]
--  }
--
function Qless.track(now, command, jid)
    if command ~= nil then
        assert(jid, 'Track(): Arg "jid" missing')
        if string.lower(ARGV[1]) == 'track' then
            redis.call('publish', 'track', jid)
            return redis.call('zadd', 'ql:tracked', now, jid)
        elseif string.lower(ARGV[1]) == 'untrack' then
            redis.call('publish', 'untrack', jid)
            return redis.call('zrem', 'ql:tracked', jid)
        else
            error('Track(): Unknown action "' .. command .. '"')
        end
    else
        local response = {
            jobs = {},
            expired = {}
        }
        local jids = redis.call('zrange', 'ql:tracked', 0, -1)
        for index, jid in ipairs(jids) do
            local data = Qless.job(jid):data()
            if data then
                table.insert(response.jobs, data)
            else
                table.insert(response.expired, jid)
            end
        end
        return response
    end
end

-- tag(0, now, ('add' | 'remove'), jid, tag, [tag, ...])
-- tag(0, now, 'get', tag, [offset, [count]])
-- tag(0, now, 'top', [offset, [count]])
-- ------------------------------------------------------------------------------------------------------------------
-- Accepts a jid, 'add' or 'remove', and then a list of tags
-- to either add or remove from the job. Alternatively, 'get',
-- a tag to get jobs associated with that tag, and offset and
-- count
--
-- If 'add' or 'remove', the response is a list of the jobs
-- current tags, or False if the job doesn't exist. If 'get',
-- the response is of the form:
--
--  {
--      total: ...,
--      jobs: [
--          jid,
--          ...
--      ]
--  }
--
-- If 'top' is supplied, it returns the most commonly-used tags
-- in a paginated fashion.
function Qless.tag(now, command, ...)
    assert(command, 'Tag(): Arg "command" must be "add", "remove", "get" or "top"')

    if command == 'add' then
        local jid  = assert(arg[1], 'Tag(): Arg "jid" missing')
        local tags = redis.call('hget', 'ql:j:' .. jid, 'tags')
        -- If the job has been canceled / deleted, then return false
        if tags then
            -- Decode the json blob, convert to dictionary
            tags = cjson.decode(tags)
            local _tags = {}
            for i,v in ipairs(tags) do _tags[v] = true end
        
            -- Otherwise, add the job to the sorted set with that tags
            for i=2,#arg do
                local tag = arg[i]
                if _tags[tag] == nil then
                    table.insert(tags, tag)
                end
                redis.call('zadd', 'ql:t:' .. tag, now, jid)
                redis.call('zincrby', 'ql:tags', 1, tag)
            end
        
            tags = cjson.encode(tags)
            redis.call('hset', 'ql:j:' .. jid, 'tags', tags)
            return tags
        else
            return false
        end
    elseif command == 'remove' then
        local jid  = assert(arg[1], 'Tag(): Arg "jid" missing')
        local tags = redis.call('hget', 'ql:j:' .. jid, 'tags')
        -- If the job has been canceled / deleted, then return false
        if tags then
            -- Decode the json blob, convert to dictionary
            tags = cjson.decode(tags)
            local _tags = {}
            for i,v in ipairs(tags) do _tags[v] = true end
        
            -- Otherwise, add the job to the sorted set with that tags
            for i=2,#arg do
                local tag = arg[i]
                _tags[tag] = nil
                redis.call('zrem', 'ql:t:' .. tag, jid)
                redis.call('zincrby', 'ql:tags', -1, tag)
            end
        
            local results = {}
            for i,tag in ipairs(tags) do if _tags[tag] then table.insert(results, tag) end end
        
            tags = cjson.encode(results)
            redis.call('hset', 'ql:j:' .. jid, 'tags', tags)
            return results
        else
            return false
        end
    elseif command == 'get' then
        local tag    = assert(arg[1], 'Tag(): Arg "tag" missing')
        local offset = assert(tonumber(arg[2] or 0),
            'Tag(): Arg "offset" not a number: ' .. tostring(arg[2]))
        local count  = assert(tonumber(arg[3] or 25),
            'Tag(): Arg "count" not a number: ' .. tostring(arg[3]))
        return {
            total = redis.call('zcard', 'ql:t:' .. tag),
            jobs  = redis.call('zrange', 'ql:t:' .. tag, offset, count)
        }
    elseif command == 'top' then
        local offset = assert(tonumber(arg[1] or 0) , 'Tag(): Arg "offset" not a number: ' .. tostring(arg[1]))
        local count  = assert(tonumber(arg[2] or 25), 'Tag(): Arg "count" not a number: ' .. tostring(arg[2]))
        return redis.call('zrevrangebyscore', 'ql:tags', '+inf', 2, 'limit', offset, count)
    else
        error('Tag(): First argument must be "add", "remove" or "get"')
    end
end

-- This script takes the name of the queue(s) and removes it
-- from the ql:paused_queues set.
--
-- Args: The list of queues to pause.
function Qless.unpause(...)
    redis.call('srem', 'ql:paused_queues', unpack(arg))
end

-- This script takes the name of the queue(s) and adds it
-- to the ql:paused_queues set.
--
-- Args: The list of queues to pause.
--
-- Note: long term, we have discussed adding a rate-limiting
-- feature to qless-core, which would be more flexible and
-- could be used for pausing (i.e. pause = set the rate to 0).
-- For now, this is far simpler, but we should rewrite this
-- in terms of the rate limiting feature if/when that is added.
function Qless.pause(...)
    redis.call('sadd', 'ql:paused_queues', unpack(arg))
end

-- Cancel(0)
-- --------------
-- Cancel a job from taking place. It will be deleted from the system, and any
-- attempts to renew a heartbeat will fail, and any attempts to complete it
-- will fail. If you try to get the data on the object, you will get nothing.
function Qless.cancel(...)
    local function cancel_set(jid, jid_set)
      if not jid_set[jid] then
        error('Cancel(): ' .. jid .. ' is a dependency of one of the jobs but is not in the provided jid set')
      end

      -- Find any stage it's associated with and remove its from that stage
      local state, queue, failure, worker = unpack(redis.call('hmget', 'ql:j:' .. jid, 'state', 'queue', 'failure', 'worker'))

      if state == 'complete' then
        return false
      else
        -- If this job has dependents, then we should probably fail
        local dependents = redis.call('smembers', 'ql:j:' .. jid .. '-dependents')
        for _, dependent_jid in ipairs(dependents) do
          cancel_set(dependent_jid, jid_set)
        end

        -- Send a message out on the appropriate channels
        local encoded = cjson.encode({
          jid    = jid,
          worker = worker,
          event  = 'canceled',
          queue  = queue
        })
        redis.call('publish', 'log', encoded)

        -- Remove this job from whatever worker has it, if any
        if worker then
          redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)
          -- If necessary, send a message to the appropriate worker, too
          redis.call('publish', worker, encoded)
        end

        -- Remove it from that queue
        if queue then
          redis.call('zrem', 'ql:q:' .. queue .. '-work', jid)
          redis.call('zrem', 'ql:q:' .. queue .. '-locks', jid)
          redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', jid)
          redis.call('zrem', 'ql:q:' .. queue .. '-depends', jid)
        end

        -- We should probably go through all our dependencies and remove ourselves
        -- from the list of dependents
        for i, j in ipairs(redis.call('smembers', 'ql:j:' .. jid .. '-dependencies')) do
          redis.call('srem', 'ql:j:' .. j .. '-dependents', jid)
        end

        -- Delete any notion of dependencies it has
        redis.call('del', 'ql:j:' .. jid .. '-dependencies')

        -- If we're in the failed state, remove all of our data
        if state == 'failed' then
          failure = cjson.decode(failure)
          -- We need to make this remove it from the failed queues
          redis.call('lrem', 'ql:f:' .. failure.group, 0, jid)
          if redis.call('llen', 'ql:f:' .. failure.group) == 0 then
            redis.call('srem', 'ql:failures', failure.group)
          end
        end

        -- Remove it as a job that's tagged with this particular tag
        local tags = cjson.decode(redis.call('hget', 'ql:j:' .. jid, 'tags') or '{}')
        for i, tag in ipairs(tags) do
          redis.call('zrem', 'ql:t:' .. tag, jid)
          redis.call('zincrby', 'ql:tags', -1, tag)
        end

        -- If the job was being tracked, we should notify
        if redis.call('zscore', 'ql:tracked', jid) ~= false then
          redis.call('publish', 'canceled', jid)
        end

        -- Just go ahead and delete our data
        redis.call('del', 'ql:j:' .. jid)
      end
    end

    -- Taken from: http://www.lua.org/pil/11.5.html
    local function to_set(list)
        local set = {}
        for _, l in ipairs(list) do set[l] = true end
        return set
    end

    local jid_set = to_set(arg)

    for _, jid in ipairs(arg) do
        cancel_set(jid, jid_set)
    end
end

-------------------------------------------------------------------------------
-- Exposed Interface
-------------------------------------------------------------------------------
local QlessAPI = {}

-- Return json for the job identified by the provided jid. If the job is not
-- present, then `nil` is returned
function QlessAPI.get(now, jid)
    local data = Qless.job(jid):data()
    if not data then
        return nil
    end
    return cjson.encode(data)
end

-- Public access
QlessAPI['config.get'] = function(now, key)
    return cjson.encode(Qless.config.get(key))
end

QlessAPI['config.set'] = function(now, key, value)
    return Qless.config.set(key, value)
end

-- Unset a configuration option
QlessAPI['config.unset'] = function(now, key)
    return Qless.config.unset(key)
end

-- Get information about a queue or queues
QlessAPI.queues = function(now, queue)
    return cjson.encode(Qless.queues(now, queue))
end

QlessAPI.complete = function(now, jid, worker, queue, data, ...)
    return Qless.job(jid):complete(now, worker, queue, data, unpack(arg))
end

QlessAPI.failed = function(now, group, start, limit)
    return cjson.encode(Qless.failed(group, start, limit))
end

QlessAPI.fail = function(now, jid, worker, group, message, data)
    return Qless.job(jid):fail(now, worker, group, message, data)
end

QlessAPI.jobs = function(now, state, ...)
    return Qless.jobs(now, state, unpack(arg))
end

QlessAPI.retry = function(now, jid, queue, worker, delay)
    return Qless.job(jid):retry(now, queue, worker, delay)
end

QlessAPI.depends = function(now, jid, command, ...)
    return Qless.job(jid):depends(command, unpack(arg))
end

QlessAPI.heartbeat = function(now, jid, worker, data)
    return Qless.job(jid):heartbeat(now, worker, data)
end

QlessAPI.workers = function(now, worker)
    return cjson.encode(Qless.workers(now, worker))
end

QlessAPI.track = function(now, command, jid)
    return cjson.encode(Qless.track(now, command, jid))
end

QlessAPI.tag = function(now, command, ...)
    return cjson.encode(Qless.tag(now, command, unpack(arg)))
end

QlessAPI.stats = function(now, queue, date)
    return cjson.encode(Qless.queue(queue):stats(now, date))
end

QlessAPI.priority = function(now, jid, priority)
    return Qless.job(jid):priority(priority)
end

QlessAPI.peek = function(now, queue, count)
    return cjson.encode(Qless.queue(queue):peek(now, count))
end

QlessAPI.pop = function(now, queue, worker, count)
    return cjson.encode(Qless.queue(queue):pop(now, worker, count))
end

QlessAPI.pause = function(now, ...)
    return Qless.pause(unpack(arg))
end

QlessAPI.unpause = function(now, ...)
    return Qless.unpause(unpack(arg))
end

QlessAPI.cancel = function(now, ...)
    return Qless.cancel(unpack(arg))
end

QlessAPI.put = function(now, queue, jid, klass, data, delay, ...)
    return Qless.queue(queue):put(now, jid, klass, data, delay, unpack(arg))
end

-------------------------------------------------------------------------------
-- Function lookup
-------------------------------------------------------------------------------

-- None of the qless function calls accept keys
if #KEYS > 0 then erorr('No Keys should be provided') end

-- The first argument must be the function that we intend to call, and it must
-- exist
local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
    QlessAPI[command_name], 'Unknown command ' .. command_name)

-- The second argument should be the current time from the requesting client
local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
    now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
