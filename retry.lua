-- retry(0, jid, queue, worker, now, [delay])
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

if #KEYS ~= 0 then
	error('Retry(): Got ' .. #KEYS .. ', expected 0')
end

local jid      = assert(ARGV[1]               , 'Retry(): Arg "jid" missing')
local queue    = assert(ARGV[2]               , 'Retry(): Arg "queue" missing')
local worker   = assert(ARGV[3]               , 'Retry(): Arg "worker" missing')
local now      = assert(tonumber(ARGV[4])     , 'Retry(): Arg "now" missing')
local delay    = assert(tonumber(ARGV[5] or 0), 'Retry(): Arg "delay" not a number: ' .. tostring(ARGV[5]))

-- Let's see what the old priority, history and tags were
local oldqueue, state, retries, oldworker, priority = unpack(redis.call('hmget', 'ql:j:' .. jid, 'queue', 'state', 'retries', 'worker', 'priority'))

-- If this isn't the worker that owns
if oldworker ~= worker or (state ~= 'running') then
	return false
end

-- Remove it from the locks key of the old queue
redis.call('zrem', 'ql:q:' .. oldqueue .. '-locks', jid)

local remaining = redis.call('hincrby', 'ql:j:' .. jid, 'remaining', -1)

-- Remove this job from the worker that was previously working it
redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)

if remaining < 0 then
	-- Now remove the instance from the schedule, and work queues for the queue it's in
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
			['worker']  = worker
		}))
	
	-- Add this type of failure to the list of failures
	redis.call('sadd', 'ql:failures', group)
	-- And add this particular instance to the failed types
	redis.call('lpush', 'ql:f:' .. group, jid)
else
	-- Put it in the queue again with a delay. Like put()
	if delay > 0 then
		redis.call('zadd', 'ql:q:' .. queue .. '-scheduled', now + delay, jid)
		redis.call('hset', 'ql:j:' .. jid, 'state', 'scheduled')
	else
		redis.call('zadd', 'ql:q:' .. queue .. '-work', priority - (now / 10000000000), jid)
		redis.call('hset', 'ql:j:' .. jid, 'state', 'waiting')
	end
end

return remaining
