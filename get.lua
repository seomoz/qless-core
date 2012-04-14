-- This gets all the data associated with the job with the
-- provided id.
--
-- Args:
--    1) jid

if #KEYS > 0 then error('Get(): No Keys should be provided') end

local jid = assert(ARGV[1], 'Get(): Arg "jid" missing')

-- Let's get all the data we can
local job = redis.call(
    'hmget', 'ql:j:' .. jid, 'jid', 'klass', 'state', 'queue', 'worker', 'priority',
	'expires', 'retries', 'remaining', 'data', 'tags', 'history', 'failure')

if not job[1] then
	return false
end
		
return cjson.encode({
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
	dependencies = redis.call('smembers', 'ql:j:' .. jid .. '-dependencies')
})
