-- Track(0)
-- Track(0, 'track', jid, now, tag, ...)
-- Track(0, 'untrack', jid, now)
-- ------------------------------------------
-- If no arguments are provided, it returns details of all currently-tracked jobs.
-- If the first argument is 'track', then it will start tracking the job associated
-- with that id, and 'untrack' stops tracking it. In this context, tracking is
-- nothing more than saving the job to a list of jobs that are considered special.
-- __Returns__ JSON:
-- 
-- 	{
-- 		'jobs': [
-- 			{
-- 				'id': ...,
-- 				# All the other details you'd get from 'get'
-- 			}, {
-- 				...
-- 			}
-- 		], 'expired': [
-- 			# These are all the jids that are completed and whose data expired
-- 			'deadbeef',
-- 			...,
-- 			...,
-- 		]
-- 	}
--

if #KEYS ~= 0 then
	error('Track(): No keys expected. Got ' .. #KEYS)
end

if ARGV[1] ~= nil then
	local jid = assert(ARGV[2]          , 'Track(): Arg "jid" missing')
	local now = assert(tonumber(ARGV[3]), 'Track(): Arg "now" missing')
	if string.lower(ARGV[1]) == 'track' then
		if #ARGV > 3 then
			local tags = cjson.decode(redis.call('hget', 'ql:j:' .. jid, 'tags'))
			for i=4,#ARGV do
				table.insert(tags, ARGV[i])
			end
			redis.call('hset', 'ql:j:' .. jid, 'tags', cjson.encode(tags))
		end
		return redis.call('zadd', 'ql:tracked', now, jid)
	elseif string.lower(ARGV[1]) == 'untrack' then
		return redis.call('zrem', 'ql:tracked', jid)
	else
		error('Track(): Unknown action "' .. ARGV[1] .. '"')
	end
else
	local response = {
		jobs = {},
		expired = {}
	}
	local jids = redis.call('zrange', 'ql:tracked', 0, -1)
	for index, jid in ipairs(jids) do
		local r = redis.call(
		    'hmget', 'ql:j:' .. jid, 'id', 'priority', 'data', 'tags', 'worker',
			'expires', 'state', 'queue', 'history', 'failure', 'retries', 'remaining', 'type')
		
		if r[1] then
			table.insert(response.jobs, {
			    id        = r[1],
			    priority  = tonumber(r[2]),
			    data      = cjson.decode(r[3]) or {},
			    tags      = cjson.decode(r[4]) or {},
			    worker    = r[5] or '',
			    expires   = tonumber(r[6]) or 0,
			    state     = r[7],
			    queue     = r[8],
			    history   = cjson.decode(r[9]),
				failure   = cjson.decode(r[10] or '{}'),
				retries   = tonumber(r[11]),
				remaining = tonumber(r[12]),
				type      = r[13]
			})
		else
			table.insert(response.expired, jid)
		end
	end
	return cjson.encode(response)
end
