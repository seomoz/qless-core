-- Workers(0, now, [worker])
-- -------------------------
-- Provide data about all the workers, or if a specific worker is provided, then
-- which jobs that worker is responsible for. If no worker is provided, expect a
-- response of the form:
-- 
-- 	[
-- 		# This is sorted by the recency of activity from that worker
-- 		{
-- 			'name'   : 'hostname1-pid1',
-- 			'jobs'   : 20,
-- 			'stalled': 0
-- 		}, {
-- 			...
-- 		}
-- 	]
-- 
-- If a worker id is provided, then expect a response of the form:
-- 
-- 	{
-- 		'jobs': [
-- 			jid1,
-- 			jid2,
-- 			...
-- 		], 'stalled': [
-- 			jid1,
-- 			...
-- 		]
-- 	}
--
if #KEYS > 0 then
	error('Workers(): No key arguments expected')
end

local now = assert(tonumber(ARGV[1]), 'Workers(): Arg "now" missing or not a number: ' .. (ARGV[1] or 'nil'))

-- Clean up all the workers' job lists if they're too old. This is determined
-- by the `max-worker-age` configuration, defaulting to the last day. Seems
-- like a 'reasonable' default
local interval = tonumber(
	redis.call('hget', 'ql:config', 'max-worker-age')) or 86400

local workers  = redis.call('zrangebyscore', 'ql:workers', 0, now - interval)
for index, worker in ipairs(workers) do
	redis.call('del', 'ql:w:' .. worker .. ':jobs')
end

-- And now remove them from the list of known workers
redis.call('zremrangebyscore', 'ql:workers', 0, now - interval)

if #ARGV == 1 then
	local response = {}
	local workers = redis.call('zrevrange', 'ql:workers', 0, -1)
	for index, worker in ipairs(workers) do
		table.insert(response, {
			name    = worker,
			jobs    = redis.call('zcount', 'ql:w:' .. worker .. ':jobs', now, now + 8640000),
			stalled = redis.call('zcount', 'ql:w:' .. worker .. ':jobs', 0, now)
		})
	end
	return cjson.encode(response)
else
	local worker = assert(ARGV[2], 'Workers(): Arg "worker" missing.')
	local response = {
		jobs    = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now + 8640000, now),
		stalled = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now, 0)
	}
	return cjson.encode(response)
end