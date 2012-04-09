-- Failed(0, [group, [start, [limit]]])
-- ------------------------------------
-- If no type is provided, this returns a JSON blob of the counts of the various
-- types of failures known. If a type is provided, it will report up to `limit`
-- from `start` of the jobs affected by that issue. __Returns__ a JSON blob.
-- 
-- 	# If no group, then...
-- 	{
-- 		'group1': 1,
-- 		'group2': 5,
-- 		...
-- 	}
-- 	
-- 	# If a type is provided, then...
-- 	{
--      'total': 20,
-- 		'jobs': [
-- 			{
-- 				# All the normal keys for a job
-- 				'jid': ...,
-- 				'data': ...
-- 				# The message for this particular instance
-- 				'message': ...,
-- 				'group': ...,
-- 			}, ...
-- 		]
-- 	}
--
-- Args:
--    1) [group]
--    2) [start]
--    3) [limit]

if #KEYS > 0 then error('Failed(): No Keys should be provided') end

local group = ARGV[1]
local start = assert(tonumber(ARGV[2] or  0), 'Failed(): Arg "start" is not a number: ' .. (ARGV[2] or 'nil'))
local limit = assert(tonumber(ARGV[3] or 25), 'Failed(): Arg "limit" is not a number: ' .. (ARGV[3] or 'nil'))

if group then
	-- If a type was provided, then we should do paginated lookup into that
	local response = {
		total = redis.call('llen', 'ql:f:' .. group),
		jobs  = {}
	}
	local jids = redis.call('lrange', 'ql:f:' .. group, start, limit)
	for index, jid in ipairs(jids) do
		local job = redis.call(
		    'hmget', 'ql:j:' .. jid, 'jid', 'priority', 'data', 'tags', 'worker', 'expires',
			'state', 'queue', 'history', 'retries', 'remaining', 'failure', 'type')
		
		table.insert(response.jobs, {
		    jid       = job[1],
		    priority  = tonumber(job[2]),
		    data      = cjson.decode(job[3]) or {},
		    tags      = cjson.decode(job[4]) or {},
		    worker    = job[5] or '',
		    expires   = tonumber(job[6]) or 0,
		    state     = job[7],
		    queue     = job[8],
		    history   = cjson.decode(job[9]),
			retries   = tonumber(job[10]),
			remaining = tonumber(job[11]),
			failure   = cjson.decode(job[12] or '{}'),
			type      = job[13]
		})
	end
	return cjson.encode(response)
else
	-- Otherwise, we should just list all the known failure types we have
	local response = {}
	local groups = redis.call('smembers', 'ql:failures')
	for index, group in ipairs(groups) do
		response[group] = redis.call('llen', 'ql:f:' .. group)
	end
	return cjson.encode(response)
end