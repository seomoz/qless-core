-- Jobs(0, ('stalled' | 'running' | 'scheduled'), now, queue)
-- ----------------------------------------------------------
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

local t     = assert(ARGV[1]          , 'Jobs(): Missing "type" argument')
local now   = assert(tonumber(ARGV[2]), 'Jobs(): Missing "now" argument')
local queue = assert(ARGV[3]          , 'Jobs(): Missing "queue" argument')

if t == 'running' then
	return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', now, 133389432700)
elseif t == 'stalled' then
	return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', 0, now)
elseif t == 'scheduled' then
	-- This will just offer 
	return redis.call('zrange', 'ql:q:' .. queue .. '-scheduled', 0, -1)
else
	error('Jobs(): Unknown type "' .. t .. '"')
end