-- Track(0)
-- Track(0, ('track' | 'untrack'), jid, now)
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
-- 				'jid': ...,
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
	local now = assert(tonumber(ARGV[3]), 'Track(): Arg "now" missing or not a number: ' .. (ARGV[3] or 'nil'))
	if string.lower(ARGV[1]) == 'track' then
		redis.call('publish', 'track', jid)
		return redis.call('zadd', 'ql:tracked', now, jid)
	elseif string.lower(ARGV[1]) == 'untrack' then
		redis.call('publish', 'untrack', jid)
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
		local job = redis.call(
		    'hmget', 'ql:j:' .. jid, 'jid', 'klass', 'state', 'queue', 'worker', 'priority',
			'expires', 'retries', 'remaining', 'data', 'tags', 'history', 'failure')
		
		if job[1] then
			table.insert(response.jobs, {
			    jid          = job[1],
				klass        = job[2],
			    state        = job[3],
			    queue        = job[4],
				worker       = job[5] or '',
				tracked      = true,
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
		else
			table.insert(response.expired, jid)
		end
	end
	return cjson.encode(response)
end
