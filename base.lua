-------------------------------------------------------------------------------
-- Forward declarations to make everything happy
-------------------------------------------------------------------------------
local Qless = {}

-- Queue forward delcaration
local QlessQueue = {}
QlessQueue.__index = QlessQueue

-- Job forward declaration
local QlessJob = {}
QlessJob.__index = QlessJob

-- RecurringJob forward declaration
local QlessRecurringJob = {}
QlessRecurringJob.__index = QlessRecurringJob

-- Config forward declaration
Qless.config = {}

-- Return a job object
function Qless.job(jid)
    assert(jid, 'Job(): no jid provided')
    local job = {}
    setmetatable(job, QlessJob)
    job.jid = jid
    return job
end

-- Return a queue object
function Qless.queue(name)
    assert(name, 'Queue(): no queue name provided')
    local queue = {}
    setmetatable(queue, QlessQueue)
    queue.name = name
    return queue
end

-- Return a recurring job object
function Qless.recurring(jid)
    assert(jid, 'Recurring(): no jid provided')
    local job = {}
    setmetatable(job, QlessRecurringJob)
    job.jid = jid
    return job
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

-- DeregisterWorkers(0, worker)
-- This script takes the name of a worker(s) on removes it/them 
-- from the ql:workers set.
--
-- Args: The list of workers to deregister.
function Qless.deregister(...)
    redis.call('zrem', 'ql:workers', unpack(arg))
end
