-- This scripts gets the requested configuration option, or in the 
-- absence of a requested option, all options.
--
-- NOTE: This serves as at the very least, a reference implementation.
--     It does not require any complex locking operations, or to
--     ensure consistency, and so it is a good candidate for getting
--     absorbed into any client library.
--
-- Args:
--    1) [option]

if #KEYS > 0 then error('GetConfig(): No Keys should be provided') end

local defaults = {
	['stats-history']      = 30,
	['histogram-history']  = 7,
	['jobs-history-count'] = 50000,
	['jobs-history']       = 604800
}

if ARGV[1] then
	return redis.call('hget', 'ql:config', ARGV[1]) or defaults[ARGV[1]]
else
	-- Inspired by redis-lua https://github.com/nrk/redis-lua/blob/version-2.0/src/redis.lua
	local reply = redis.call('hgetall', 'ql:config')
    for i = 1, #reply, 2 do defaults[reply[i]] = reply[i + 1] end
	return cjson.encode(defaults)
end
