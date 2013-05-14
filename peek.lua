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

if #KEYS ~= 1 then
	if #KEYS < 1 then
		error('Peek(): Expected 1 KEYS argument')
	else
		error('Peek(): Got ' .. #KEYS .. ', expected 1 KEYS argument')
	end
end

local queue   = assert(KEYS[1]           , 'Peek(): Key "queue" missing')
local key     = 'ql:q:' .. queue
local count   = assert(tonumber(ARGV[1]) , 'Peek(): Arg "count" missing or not a number: ' .. (ARGV[1] or 'nil'))
local now     = assert(tonumber(ARGV[2]) , 'Peek(): Arg "now" missing or not a number: ' .. (ARGV[2] or 'nil'))

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
		while (score <= now) and (moved < (count - #keys)) do
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

-- Now we've checked __all__ the locks for this queue the could
-- have expired, and are no more than the number requested. If
-- we still need values in order to meet the demand, then we 
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
for index, jid in ipairs(keys) do
	local job = redis.call(
	    'hmget', 'ql:j:' .. jid, 'jid', 'klass', 'state', 'queue', 'worker', 'priority',
		'expires', 'retries', 'remaining', 'data', 'tags', 'history', 'failure')
	
	table.insert(response, cjson.encode({
	    jid          = job[1],
		klass        = job[2],
	    state        = job[3],
	    queue        = job[4],
		worker       = job[5] or '',
		tracked      = redis.call('zscore', 'ql:tracked', jid) ~= false,
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
		dependencies = {}
		
	}))
end

return response
