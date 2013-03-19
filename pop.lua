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

if #KEYS ~= 1 then
	if #KEYS < 1 then
		error('Pop(): Expected 1 KEYS argument')
	else
		error('Pop(): Got ' .. #KEYS .. ', expected 1 KEYS argument')
	end
end

local queue   = assert(KEYS[1]           , 'Pop(): Key "queue" missing')
local key     = 'ql:q:' .. queue
local worker  = assert(ARGV[1]           , 'Pop(): Arg "worker" missing')
local count   = assert(tonumber(ARGV[2]) , 'Pop(): Arg "count" missing or not a number: ' .. (ARGV[2] or 'nil'))
local now     = assert(tonumber(ARGV[3]) , 'Pop(): Arg "now" missing or not a number: '   .. (ARGV[3] or 'nil'))

-- We should find the heartbeat interval for this queue
-- heartbeat
local _hb, _qhb = unpack(redis.call('hmget', 'ql:config', 'heartbeat', queue .. '-heartbeat'))
local expires = now + tonumber(_qhb or _hb or 60)

-- The bin is midnight of the provided day
-- 24 * 60 * 60 = 86400
local bin = now - (now % 86400)

-- These are the ids that we're going to return
local keys = {}

-- Make sure we this worker to the list of seen workers
redis.call('zadd', 'ql:workers', now, worker)

if redis.call('sismember', 'ql:paused_queues', queue) == 1 then
  return {}
end

-- Iterate through all the expired locks and add them to the list
-- of keys that we'll return
for index, jid in ipairs(redis.call('zrangebyscore', key .. '-locks', 0, now, 'LIMIT', 0, count)) do
	-- Remove this job from the jobs that the worker that was running it has
	local w = redis.call('hget', 'ql:j:' .. jid, 'worker')
	redis.call('zrem', 'ql:w:' .. w .. ':jobs', jid)
	
	-- For each of these, decrement their retries. If any of them
	-- have exhausted their retries, then we should mark them as
	-- failed.
	if redis.call('hincrby', 'ql:j:' .. jid, 'remaining', -1) < 0 then
		-- Now remove the instance from the schedule, and work queues for the queue it's in
		redis.call('zrem', 'ql:q:' .. queue .. '-work', jid)
		redis.call('zrem', 'ql:q:' .. queue .. '-locks', jid)
		redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', jid)
		
		local group = 'failed-retries-' .. queue
		-- First things first, we should get the history
		local history = redis.call('hget', 'ql:j:' .. jid, 'history')
		
		-- Now, take the element of the history for which our provided worker is the worker, and update 'failed'
		history = cjson.decode(history or '[]')
		history[#history]['failed'] = now
		
		redis.call('hmset', 'ql:j:' .. jid, 'state', 'failed', 'worker', '',
			'expires', '', 'history', cjson.encode(history), 'failure', cjson.encode({
				['group']   = group,
				['message'] = 'Job exhausted retries in queue "' .. queue .. '"',
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
redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'retries', #keys)

-- If we still need jobs in order to meet demand, then we should
-- look for all the recurring jobs that need jobs run
if #keys < count then
	-- This is how many jobs we've moved so far
	local moved = 0
	-- These are the recurring jobs that need work
	local r = redis.call('zrangebyscore', key .. '-recur', 0, now, 'LIMIT', 0, (count - #keys))
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

		while (score <= now) and (moved < (count - #keys)) do
			-- Increment the count of how many jobs we've moved from recurring
			-- to 'work'
			moved = moved + 1

			-- the count'th job that we've moved from this recurring job
			local count = redis.call('hincrby', 'ql:r:' .. jid, 'count', 1)
			
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
				'queue'    , queue,
				'retries'  , retries,
				'remaining', retries,
				'history'  , cjson.encode({{
					-- The job was essentially put in this queue at this time,
					-- and not the current time
					q     = queue,
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
	local count, mean, vk = unpack(redis.call('hmget', 'ql:s:wait:' .. bin .. ':' .. queue, 'total', 'mean', 'vk'))
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
		redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. queue, 's' .. waiting, 1)
	elseif waiting < 3600 then -- minutes
		redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. queue, 'm' .. math.floor(waiting / 60), 1)
	elseif waiting < 86400 then -- hours
		redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. queue, 'h' .. math.floor(waiting / 3600), 1)
	else -- days
		redis.call('hincrby', 'ql:s:wait:' .. bin .. ':' .. queue, 'd' .. math.floor(waiting / 86400), 1)
	end		
	redis.call('hmset', 'ql:s:wait:' .. bin .. ':' .. queue, 'total', count, 'mean', mean, 'vk', vk)
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
	
	table.insert(response, cjson.encode({
	    jid          = job[1],
		klass        = job[2],
	    state        = job[3],
	    queue        = job[4],
		worker       = job[5] or '',
		tracked      = tracked,
		priority     = tonumber(job[6]),
		expires      = tonumber(job[7]) or 0,
		retries      = tonumber(job[8]),
		remaining    = tonumber(job[9]),
		data         = cjson.decode(job[10]),
		tags         = cjson.decode(job[11]),
	    history      = cjson.decode(job[12]),
		failure      = cjson.decode(job[13] or '{}'),
		dependents   = redis.call('smembers', 'ql:j:' .. jid .. '-dependents'),
		-- A job in the waiting state can not have dependencies
		-- because it has been popped off of a queue, which 
		-- means all of its dependencies have been satisfied
		dependencies = {}
		
	}))
end

if #keys > 0 then
	redis.call('zrem', key .. '-work', unpack(keys))
end

return response
