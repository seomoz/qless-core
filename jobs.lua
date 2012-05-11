-- Jobs(0, 'complete' | (('stalled' | 'running' | 'scheduled' | 'depends', 'recurring'), now, queue) [offset, [count]])
-- -------------------------------------------------------------------------------------------------------
-- 
-- Return all the job ids currently considered to be in the provided state
-- in a particular queue. The response is a list of job ids:
-- 
-- 	[
--		jid1, 
--		jid2,
--		...
--	]

if #KEYS > 0 then
	error('Jobs(): Got '.. #KEYS .. ' expected 0 KEYS arguments')
end

local t      = assert(ARGV[1]                , 'Jobs(): Arg "type" missing')
if t == 'complete' then
	local offset = assert(tonumber(ARGV[2] or 0) , 'Jobs(): Arg "offset" not a number: ' .. tostring(ARGV[2]))
	local count  = assert(tonumber(ARGV[3] or 25), 'Jobs(): Arg "count" not a number: ' .. tostring(ARGV[3]))
	return redis.call('zrevrange', 'ql:completed', offset, offset + count - 1)
else
	local now    = assert(tonumber(ARGV[2])      , 'Jobs(): Arg "now" missing or not a number: ' .. tostring(ARGV[2]))
	local queue  = assert(ARGV[3]                , 'Jobs(): Arg "queue" missing')
	local offset = assert(tonumber(ARGV[4] or 0) , 'Jobs(): Arg "offset" not a number: ' .. tostring(ARGV[4]))
	local count  = assert(tonumber(ARGV[5] or 25), 'Jobs(): Arg "count" not a number: ' .. tostring(ARGV[5]))

	if t == 'running' then
		return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', now, 133389432700, 'limit', offset, count)
	elseif t == 'stalled' then
		return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', 0, now, 'limit', offset, count)
	elseif t == 'scheduled' then
		return redis.call('zrange', 'ql:q:' .. queue .. '-scheduled', offset, offset + count - 1)
	elseif t == 'depends' then
		return redis.call('zrange', 'ql:q:' .. queue .. '-depends', offset, offset + count - 1)
	elseif t == 'recurring' then
		return redis.call('zrange', 'ql:q:' .. queue .. '-recur', offset, offset + count - 1)
	else
		error('Jobs(): Unknown type "' .. t .. '"')
	end
end