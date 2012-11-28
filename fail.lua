-- Fail(0, jid, worker, group, message, now, [data])
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

if #KEYS > 0 then error('Fail(): No Keys should be provided') end

local jid     = assert(ARGV[1]          , 'Fail(): Arg "jid" missing')
local worker  = assert(ARGV[2]          , 'Fail(): Arg "worker" missing')
local group   = assert(ARGV[3]          , 'Fail(): Arg "group" missing')
local message = assert(ARGV[4]          , 'Fail(): Arg "message" missing')
local now     = assert(tonumber(ARGV[5]), 'Fail(): Arg "now" missing or malformed: ' .. (ARGV[5] or 'nil'))
local data    = ARGV[6]

-- The bin is midnight of the provided day
-- 24 * 60 * 60 = 86400
local bin = now - (now % 86400)

if data then
	data = cjson.decode(data)
end

-- First things first, we should get the history
local history, queue, state = unpack(redis.call('hmget', 'ql:j:' .. jid, 'history', 'queue', 'state'))

-- If the job has been completed, we cannot fail it
if state ~= 'running' then
	return false
end

if redis.call('zscore', 'ql:tracked', jid) ~= false then
	redis.call('publish', 'failed', jid)
end

-- Remove this job from the jobs that the worker that was running it has
redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)

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
redis.call('zrem', 'ql:q:' .. queue .. '-work', jid)
redis.call('zrem', 'ql:q:' .. queue .. '-locks', jid)
redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', jid)

-- The reason that this appears here is that the above will fail if the job doesn't exist
if data then
	redis.call('hset', 'ql:j:' .. jid, 'data', cjson.encode(data))
end

redis.call('hmset', 'ql:j:' .. jid, 'state', 'failed', 'worker', '',
	'expires', '', 'history', cjson.encode(history), 'failure', cjson.encode({
		['group']   = group,
		['message'] = message,
		['when']    = math.floor(now),
		['worker']  = worker
	}))

-- Add this group of failure to the list of failures
redis.call('sadd', 'ql:failures', group)
-- And add this particular instance to the failed groups
redis.call('lpush', 'ql:f:' .. group, jid)

-- Here is where we'd intcrement stats about the particular stage
-- and possibly the workers

return jid