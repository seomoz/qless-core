-- Queues(0, now, [queue])
-- -----------------------
-- 
-- Return all the queues we know about, with how many jobs are scheduled, waiting,
-- and running in that queue. If a queue name is provided, then only the appropriate
-- response hash should be returned. The response is JSON:
-- 
-- 	[
-- 		{
-- 			'name': 'testing',
-- 			'stalled': 2,
-- 			'waiting': 5,
-- 			'running': 5,
-- 			'scheduled': 10,
-- 			'depends': 5,
-- 			'recurring': 0
-- 		}, {
-- 			...
-- 		}
-- 	]

if #KEYS > 0 then
	error('Queues(): Got '.. #KEYS .. ' expected 0 KEYS arguments')
end

local now   = assert(tonumber(ARGV[1]), 'Queues(): Arg "now" missing or not a number: ' .. (ARGV[1] or 'nil'))
local queue = ARGV[2]

local response = {}
local queuenames = redis.call('zrange', 'ql:queues', 0, -1)

if queue then
	local stalled = redis.call('zcount', 'ql:q:' .. queue .. '-locks', 0, now)
	response = {
		name      = queue,
		waiting   = redis.call('zcard', 'ql:q:' .. queue .. '-work'),
		stalled   = stalled,
		running   = redis.call('zcard', 'ql:q:' .. queue .. '-locks') - stalled,
		scheduled = redis.call('zcard', 'ql:q:' .. queue .. '-scheduled'),
		depends   = redis.call('zcard', 'ql:q:' .. queue .. '-depends'),
		recurring = redis.call('zcard', 'ql:q:' .. queue .. '-recur')
	}
else
	for index, qname in ipairs(queuenames) do
		local stalled = redis.call('zcount', 'ql:q:' .. qname .. '-locks', 0, now)
		table.insert(response, {
			name      = qname,
			waiting   = redis.call('zcard', 'ql:q:' .. qname .. '-work'),
			stalled   = stalled,
			running   = redis.call('zcard', 'ql:q:' .. qname .. '-locks') - stalled,
			scheduled = redis.call('zcard', 'ql:q:' .. qname .. '-scheduled'),
			depends   = redis.call('zcard', 'ql:q:' .. qname .. '-depends'),
			recurring = redis.call('zcard', 'ql:q:' .. qname .. '-recur')
		})
	end
end

return cjson.encode(response)