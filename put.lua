-- Put(1, queue, id, data, now, [priority, [tags, [delay, [retries]]]])
-- --------------------------------------------------------------------
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
-- Keys:
--    1) queue name
-- Args:
--    1) id
--    2) type
--    3) data
--    4) now
--    5) [priority]
--    6) [tags]
--    7) [delay]
--    8) [retries]

if #KEYS ~= 1 then
	if #KEYS < 1 then
		error('Put(): Expected 1 KEYS argument')
	else
		error('Put(): Got ' .. #KEYS .. ', expected 1 KEYS argument')
	end
end

local queue    = assert(KEYS[1]               , 'Put(): Key "queue" missing')
local id       = assert(ARGV[1]               , 'Put(): Arg "id" missing')
local t        = assert(ARGV[2]               , 'Put(): Arg "type" missing')
local data     = assert(cjson.decode(ARGV[3]) , 'Put(): Arg "data" missing')
local now      = assert(tonumber(ARGV[4])     , 'Put(): Arg "now" missing')
local delay    = assert(tonumber(ARGV[7] or 0), 'Put(): Arg "delay" not a number')
local retries  = assert(tonumber(ARGV[8] or 5), 'Put(): Arg "retries" not a number')

-- Let's see what the old priority, history and tags were
local history, priority, tags, oldqueue, state, failure, _retries, worker = unpack(redis.call('hmget', 'ql:j:' .. id, 'history', 'priority', 'tags', 'queue', 'state', 'failure', 'retries', 'worker'))

-- If no retries are provided, then we should use
-- whatever the job previously had
if ARGV[8] == nil then
	retries = _retries or 5
end

-- Update the history to include this new change
local history = cjson.decode(history or '{}')
table.insert(history, {
	q     = queue,
	put   = now
})

-- And make sure that the tags are either what was provided or the existing
tags     = assert(cjson.decode(ARGV[6] or tags or '[]'), 'Put(): Arg "tags" not JSON')

-- And make sure that the priority is ok
priority = assert(tonumber(ARGV[5] or priority or 0), 'Put(): Arg "priority" not a number')

-- If this item was previously in another queue, then we should remove it from there
if oldqueue then
	redis.call('zrem', 'ql:q:' .. oldqueue .. '-work', id)
	redis.call('zrem', 'ql:q:' .. oldqueue .. '-locks', id)
	redis.call('zrem', 'ql:q:' .. oldqueue .. '-scheduled', id)
end

-- If this had previously been given out to a worker,
-- make sure to remove it from that worker's jobs
if worker then
	redis.call('zrem', 'ql:w:' .. worker .. ':jobs', id)
end

-- If the job was previously in the 'completed' state, then we should remove
-- it from being enqueued for destructination
if state == 'completed' then
	redis.call('zrem', 'ql:completed', id)
end

-- If we're in the failed state, remove all of our data
if state == 'failed' then
	failure = cjson.decode(failure)
	-- We need to make this remove it from the failed queues
	redis.call('lrem', 'ql:f:' .. failure.type, 0, id)
	if redis.call('llen', 'ql:f:' .. failure.type) == 0 then
		redis.call('srem', 'ql:failures', failure.type)
	end
	-- The bin is midnight of the provided day
	-- 24 * 60 * 60 = 86400
	local bin = failure.when - (failure.when % 86400)
	-- We also need to decrement the stats about the queue on
	-- the day that this failure actually happened.
	redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed'  , -1)
end

-- First, let's save its data
redis.call('hmset', 'ql:j:' .. id,
    'id'       , id,
	'type'     , t,
    'data'     , cjson.encode(data),
    'priority' , priority,
    'tags'     , cjson.encode(tags),
    'state'    , 'waiting',
    'worker'   , '',
	'expires'  , 0,
    'queue'    , queue,
	'retries'  , retries,
	'remaining', retries,
    'history'  , cjson.encode(history))

-- Now, if a delay was provided, and if it's in the future,
-- then we'll have to schedule it. Otherwise, we're just
-- going to add it to the work queue.
if delay > 0 then
    redis.call('zadd', 'ql:q:' .. queue .. '-scheduled', now + delay, id)
else
    redis.call('zadd', 'ql:q:' .. queue .. '-work', priority, id)
end

-- Lastly, we're going to make sure that this item is in the
-- set of known queues. We should keep this sorted by the 
-- order in which we saw each of these queues
if redis.call('zscore', 'ql:queues', queue) == false then
	redis.call('zadd', 'ql:queues', now, queue)
end

return id