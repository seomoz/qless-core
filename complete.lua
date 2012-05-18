-- Complete(0, jid, worker, queue, now, data, [next, q, [(delay, d) | (depends, '["jid1","jid2",...]')])
-- -----------------------------------------------------------------------------------------------------
-- Complete a job and optionally put it in another queue, either scheduled or to
-- be considered waiting immediately. It can also optionally accept other jids
-- on which this job will be considered dependent before it's considered valid.
--
-- Args:
--    1) jid
--    2) worker
--    3) queue
--    4) now
--    5) data
--    *) [next, q, [delay, d]], [depends, '...']

if #KEYS > 0 then error('Complete(): No Keys should be provided') end

local jid    = assert(ARGV[1]               , 'Complete(): Arg "jid" missing.')
local worker = assert(ARGV[2]               , 'Complete(): Arg "worker" missing.')
local queue  = assert(ARGV[3]               , 'Complete(): Arg "queue" missing.')
local now    = assert(tonumber(ARGV[4])     , 'Complete(): Arg "now" not a number or missing: ' .. tostring(ARGV[4]))
local data   = assert(cjson.decode(ARGV[5]) , 'Complete(): Arg "data" missing or not JSON: '    .. tostring(ARGV[5]))

-- Read in all the optional parameters
local options = {}
for i = 6, #ARGV, 2 do options[ARGV[i]] = ARGV[i + 1] end

-- Sanity check on optional args
local nextq   = options['next']
local delay   = assert(tonumber(options['delay'] or 0))
local depends = assert(cjson.decode(options['depends'] or '[]'), 'Complete(): Arg "depends" not JSON: ' .. tostring(options['depends']))

-- Delay and depends are not allowed together
if delay > 0 and #depends > 0 then
	error('Complete(): "delay" and "depends" are not allowed to be used together')
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
local lastworker, history, state, priority, retries = unpack(redis.call('hmget', 'ql:j:' .. jid, 'worker', 'history', 'state', 'priority', 'retries', 'dependents'))

if (lastworker ~= worker) or (state ~= 'running') then
	return false
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
	redis.call('hset', 'ql:j:' .. jid, 'data', cjson.encode(data))
end

-- Remove the job from the previous queue
redis.call('zrem', 'ql:q:' .. queue .. '-work', jid)
redis.call('zrem', 'ql:q:' .. queue .. '-locks', jid)
redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', jid)

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
redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)

if redis.call('zscore', 'ql:tracked', jid) ~= false then
	redis.call('publish', 'completed', jid)
end

if nextq then
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
	
	redis.call('hmset', 'ql:j:' .. jid, 'state', 'waiting', 'worker', '', 'failure', '{}',
		'queue', nextq, 'expires', 0, 'history', cjson.encode(history), 'remaining', tonumber(retries))
	
	if delay > 0 then
	    redis.call('zadd', 'ql:q:' .. nextq .. '-scheduled', now + delay, jid)
		return 'scheduled'
	else
		-- These are the jids we legitimately have to wait on
		local count = 0
		for i, j in ipairs(depends) do
			-- Make sure it's something other than 'nil' or complete.
			local state = redis.call('hget', 'ql:j:' .. j, 'state')
			if (state and state ~= 'complete') then
				count = count + 1
				redis.call('sadd', 'ql:j:' .. j .. '-dependents'  , jid)
				redis.call('sadd', 'ql:j:' .. jid .. '-dependencies', j)
			end
		end
		if count > 0 then
			redis.call('zadd', 'ql:q:' .. nextq .. '-depends', now, jid)
			redis.call('hset', 'ql:j:' .. jid, 'state', 'depends')
			return 'depends'
		else
			redis.call('zadd', 'ql:q:' .. nextq .. '-work', priority - (now / 10000000000), jid)
			return 'waiting'
		end
	end
else
	redis.call('hmset', 'ql:j:' .. jid, 'state', 'complete', 'worker', '', 'failure', '{}',
		'queue', '', 'expires', 0, 'history', cjson.encode(history), 'remaining', tonumber(retries))
	
	-- Do the completion dance
	local count, time = unpack(redis.call('hmget', 'ql:config', 'jobs-history-count', 'jobs-history'))
	
	-- These are the default values
	count = tonumber(count or 50000)
	time  = tonumber(time  or 7 * 24 * 60 * 60)
	
	-- Schedule this job for destructination eventually
	redis.call('zadd', 'ql:completed', now, jid)
	
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
	for i, j in ipairs(redis.call('smembers', 'ql:j:' .. jid .. '-dependents')) do
		redis.call('srem', 'ql:j:' .. j .. '-dependencies', jid)
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
	redis.call('del', 'ql:j:' .. jid .. '-dependents')
	
	return 'complete'
end
