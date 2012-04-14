-- Jobs(0, ('stalled' | 'running' | 'scheduled' | 'depends'), now, queue)
-- ----------------------------------------------------------------------
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

local t     = assert(ARGV[1]          , 'Jobs(): Arg "type" missing')
local now   = assert(tonumber(ARGV[2]), 'Jobs(): Arg "now" missing or not a number: ' .. (ARGV[2] or 'nil'))
local queue = assert(ARGV[3]          , 'Jobs(): Arg "queue" missing')

if t == 'running' then
	return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', now, 133389432700)
elseif t == 'stalled' then
	return redis.call('zrangebyscore', 'ql:q:' .. queue .. '-locks', 0, now)
elseif t == 'scheduled' then
	return redis.call('zrange', 'ql:q:' .. queue .. '-scheduled', 0, -1)
elseif t == 'depends' then
	return redis.call('zrange', 'ql:q:' .. queue .. '-depends', 0, -1)
else
	error('Jobs(): Unknown type "' .. t .. '"')
end