-- This scripts conducts a heartbeat for a job, and returns
-- either the new expiration or False if the lock has been
-- given to another node
--
-- Args:
--    1) ID
--    2) worker
--    3) now
--    4) [data]

if #KEYS > 0 then error('Heartbeat(): No Keys should be provided') end

local id     = assert(ARGV[1]          , 'Heartbeat(): Arg "id" missing')
local worker = assert(ARGV[2]          , 'Heartbeat(): Arg "worker" missing')
local now    = assert(tonumber(ARGV[3]), 'Heartbeat(): Arg "now" missing')
local data   = ARGV[4]

-- We should find the heartbeat interval for this queue
-- heartbeat. First, though, we need to find the queue
-- this particular job is in
local queue     = redis.call('hget', 'ql:j:' .. id, 'queue') or ''
local _hb, _qhb = unpack(redis.call('hmget', 'ql:config', 'heartbeat', 'heartbeat-' .. queue))
local expires   = now + tonumber(_qhb or _hb or 60)

if data then
	data = cjson.decode(data)
end

-- First, let's see if the worker still owns this job
if redis.call('hget', 'ql:j:' .. id, 'worker') ~= worker then
    return false
else
    -- Otherwise, optionally update the user data, and the heartbeat
    if data then
        -- I don't know if this is wise, but I'm decoding and encoding
        -- the user data to hopefully ensure its sanity
        redis.call('hmset', 'ql:j:' .. id, 'expires', expires, 'worker', worker, 'data', cjson.encode(data))
    else
        redis.call('hmset', 'ql:j:' .. id, 'expires', expires, 'worker', worker)
    end
	
	-- Update hwen this job was last updated on that worker
	-- Add this job to the list of jobs handled by this worker
	redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, id)
	
    -- And now we should just update the locks
    local queue = redis.call('hget', 'ql:j:' .. id, 'queue')
    redis.call('zadd', 'ql:q:'.. queue, expires, id)
    return expires
end
