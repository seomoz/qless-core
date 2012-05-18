-- Put(1, queue, jid, klass, data, now, delay, [priority, p], [tags, t], [retries, r], [depends, '[...]'])
-- -------------------------------------------------------------------------------------------------------
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
--    1) jid
--    2) klass
--    3) data
--    4) now
--    5) delay
--    *) [priority, p], [tags, t], [retries, r], [depends, '[...]']

if #KEYS ~= 1 then
	if #KEYS < 1 then
		error('Put(): Expected 1 KEYS argument')
	else
		error('Put(): Got ' .. #KEYS .. ', expected 1 KEYS argument')
	end
end

local queue    = assert(KEYS[1]               , 'Put(): Key "queue" missing')
local jid      = assert(ARGV[1]               , 'Put(): Arg "jid" missing')
local klass    = assert(ARGV[2]               , 'Put(): Arg "klass" missing')
local data     = assert(cjson.decode(ARGV[3]) , 'Put(): Arg "data" missing or not JSON: '    .. tostring(ARGV[3]))
local now      = assert(tonumber(ARGV[4])     , 'Put(): Arg "now" missing or not a number: ' .. tostring(ARGV[4]))
local delay    = assert(tonumber(ARGV[5])     , 'Put(): Arg "delay" not a number: '          .. tostring(ARGV[5]))

-- Read in all the optional parameters
local options = {}
for i = 6, #ARGV, 2 do options[ARGV[i]] = ARGV[i + 1] end

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

-- Update the history to include this new change
local history = cjson.decode(history or '{}')
table.insert(history, {
	q     = queue,
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
	redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed'  , -1)
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
    'queue'    , queue,
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
    redis.call('zadd', 'ql:q:' .. queue .. '-scheduled', now + delay, jid)
else
	if redis.call('scard', 'ql:j:' .. jid .. '-dependencies') > 0 then
		redis.call('zadd', 'ql:q:' .. queue .. '-depends', now, jid)
		redis.call('hset', 'ql:j:' .. jid, 'state', 'depends')
	else
		redis.call('zadd', 'ql:q:' .. queue .. '-work', priority - (now / 10000000000), jid)
	end
end

-- Lastly, we're going to make sure that this item is in the
-- set of known queues. We should keep this sorted by the 
-- order in which we saw each of these queues
if redis.call('zscore', 'ql:queues', queue) == false then
	redis.call('zadd', 'ql:queues', now, queue)
end

if redis.call('zscore', 'ql:tracked', jid) ~= false then
	redis.call('publish', 'put', jid)
end

return jid