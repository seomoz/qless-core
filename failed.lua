-- Failed(0, [group, [start, [limit]]])
-- ------------------------------------
-- If no group is provided, this returns a JSON blob of the counts of the various
-- groups of failures known. If a group is provided, it will report up to `limit`
-- from `start` of the jobs affected by that issue. __Returns__ a JSON blob.
-- 
-- 	# If no group, then...
-- 	{
-- 		'group1': 1,
-- 		'group2': 5,
-- 		...
-- 	}
-- 	
-- 	# If a group is provided, then...
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
	-- If a group was provided, then we should do paginated lookup into that
	local response = {
		total = redis.call('llen', 'ql:f:' .. group),
		jobs  = {}
	}
	local jids = redis.call('lrange', 'ql:f:' .. group, start, limit - 1)
	for index, jid in ipairs(jids) do
		local job = redis.call(
		    'hmget', 'ql:j:' .. jid, 'jid', 'klass', 'state', 'queue', 'worker', 'priority',
			'expires', 'retries', 'remaining', 'data', 'tags', 'history', 'failure')
		
		table.insert(response.jobs, {
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
			-- A job in the failed state can not have dependencies
			-- because it has been popped off of a queue, which 
			-- means all of its dependencies have been satisfied
			dependencies = {}
		})
	end
	return cjson.encode(response)
else
	-- Otherwise, we should just list all the known failure groups we have
	local response = {}
	local groups = redis.call('smembers', 'ql:failures')
	for index, group in ipairs(groups) do
		response[group] = redis.call('llen', 'ql:f:' .. group)
	end
	return cjson.encode(response)
end