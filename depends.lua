-- Depends(0, jid, ('on', [jid, [jid, [...]]]) | ('off', ('all' | [jid, [jid, [...]]]))
-- ------------------------------------------------------------------------------------
-- Add or remove dependencies a job has. If 'on' is provided, the provided jids are 
-- added as dependencies. If 'off' and 'all' are provided, then all the current dependencies
-- are removed. If 'off' is provided and the next argument is not 'all', then those
-- jids are removed as dependencies.
--
-- If a job is not already in the 'depends' state, then this call will return false.
-- Otherwise, it will return true
--
-- Args:
--    1) jid

if #KEYS > 0 then error('Depends(): No Keys should be provided') end

local jid     = assert(ARGV[1], 'Depends(): Arg "jid" missing.')
local now     = assert(tonumber(ARGV[2])     , 'Depends(): Arg "now" missing or not a number: ' .. (ARGV[2] or 'nil'))
local command = assert(ARGV[3], 'Depends(): Arg "command" missing')

if ARGV[3] == 'on' then
	-- These are the jids we legitimately have to wait on
	for i=4,#ARGV do
		local j = ARGV[i]
		-- Make sure it's something other than 'nil' or complete.
		local state = redis.call('hget', 'ql:j:' .. j, 'state')
		local q = redis.call('hget', 'ql:j:' .. jid, 'queue')
		if (state and state ~= 'complete') then
			redis.call('sadd', 'ql:j:' .. j .. '-dependents'  , jid)
			redis.call('sadd', 'ql:j:' .. jid .. '-dependencies', j)
			-- Added to correctly support newly dependent jobs	
			redis.call('zadd', 'ql:q:' .. q .. '-depends', now, jid)
			redis.call('hset', 'ql:j:' .. jid, 'state', 'depends')
		end
	end
	return true
elseif ARGV[3] == 'off' then
	if redis.call('hget', 'ql:j:' .. jid, 'state') ~= 'depends' then
		return false
	end

	if ARGV[4] == 'all' then
		for i, j in ipairs(redis.call('smembers', 'ql:j:' .. jid .. '-dependencies')) do
			redis.call('srem', 'ql:j:' .. j .. '-dependents', jid)
		end
		redis.call('del', 'ql:j:' .. jid .. '-dependencies')
		local q, p = unpack(redis.call('hmget', 'ql:j:' .. jid, 'queue', 'priority'))
		if q then
			redis.call('zrem', 'ql:q:' .. q .. '-depends', jid)
			redis.call('zadd', 'ql:q:' .. q .. '-work', p, jid)
			redis.call('hset', 'ql:j:' .. jid, 'state', 'waiting')
		end
	else
		for i=4,#ARGV do
			local j = ARGV[i]
			redis.call('srem', 'ql:j:' .. j .. '-dependents', jid)
			redis.call('srem', 'ql:j:' .. jid .. '-dependencies', j)
			if redis.call('scard', 'ql:j:' .. jid .. '-dependencies') == 0 then
				local q, p = unpack(redis.call('hmget', 'ql:j:' .. jid, 'queue', 'priority'))
				if q then
					redis.call('zrem', 'ql:q:' .. q .. '-depends', jid)
					redis.call('zadd', 'ql:q:' .. q .. '-work', p, jid)
					redis.call('hset', 'ql:j:' .. jid, 'state', 'waiting')
				end
			end
		end
	end
	return true
else
	error('Depends(): Arg "command" must be "on" or "off"')
end
