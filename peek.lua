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

local key     = assert('ql:q:' .. KEYS[1], 'Peek(): Key "queue" missing')
local count   = assert(tonumber(ARGV[1]) , 'Peek(): Arg "count" missing or not a number: ' .. (ARGV[2] or 'nil'))
local now     = assert(tonumber(ARGV[2]) , 'Peek(): Arg "now" missing or not a number: ' .. (ARGV[3] or 'nil'))

-- These are the ids that we're going to return
local keys = {}

-- Iterate through all the expired locks and add them to the list
-- of keys that we'll return
for index, jid in ipairs(redis.call('zrangebyscore', key .. '-locks', 0, now, 'LIMIT', 0, count)) do
    table.insert(keys, jid)
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
    end
    
	if #zadd > 0 then
	    -- Now add these to the work list, and then remove them
	    -- from the scheduled list
	    redis.call('zadd', key .. '-work', unpack(zadd))
	    redis.call('zrem', key .. '-scheduled', unpack(r))
	end
    
    -- And now we should get up to the maximum number of requested
    -- work items from the work queue.
    for index, jid in ipairs(redis.call('zrange', key .. '-work', 0, (count - #keys) - 1)) do
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
	    jid       = job[1],
		klass     = job[2],
	    state     = job[3],
	    queue     = job[4],
		worker    = job[5] or '',
		tracked   = redis.call('zscore', 'ql:tracked', jid) ~= false,
		priority  = tonumber(job[6]),
		expires   = tonumber(job[7]) or 0,
		retries   = tonumber(job[8]),
		remaining = tonumber(job[9]),
		data      = cjson.decode(job[10]),
		tags      = cjson.decode(job[11]),
	    history   = cjson.decode(job[12]),
		failure   = cjson.decode(job[13] or '{}'),
	}))
end

return response