-- Queues(0, now)
-- --------------
-- 
-- Return all the queues we know about, with how many jobs are scheduled, waiting,
-- and running in that queue. The response is JSON:
-- 
-- 	[
-- 		{
-- 			'name': 'testing',
--          'stalled': 2,
-- 			'waiting': 5,
-- 			'running': 5,
-- 			'scheduled': 10
-- 		}, {
-- 			...
-- 		}
-- 	]

if #KEYS > 0 then
	error('Queues(): Got '.. #KEYS .. ' expected 0 KEYS arguments')
end

local now = assert(tonumber(ARGV[1]), 'Queues(): Missing "now" argument')

local response = {}
local queuenames = redis.call('zrange', 'ql:queues', 0, -1)

for index, qname in ipairs(queuenames) do
	local stalled = redis.call('zcount', 'ql:q:' .. qname .. '-locks', 0, now)
	table.insert(response, {
		name      = qname,
		waiting   = redis.call('zcard', 'ql:q:' .. qname .. '-work'),
		stalled   = stalled,
		running   = redis.call('zcard', 'ql:q:' .. qname .. '-locks') - stalled,
		scheduled = redis.call('zcard', 'ql:q:' .. qname .. '-scheduled')
	})
end

return cjson.encode(response)
