-- config(0, 'get', [option])
-- config(0, 'set', option, value)
-- config(0, 'unset', option)
-- -------------------------------------------------------------------------------------------------------
-- This script provides an interface to get, set, and unset configuration
-- options.
--
-- Args:
--    1) [option]

if #KEYS > 0 then error('Config(): No keys should be provided') end

local command = ARGV[1]

local defaults = {
	['application']        = 'qless',
	['heartbeat']          = 60,
	['stats-history']      = 30,
	['histogram-history']  = 7,
	['jobs-history-count'] = 50000,
	['jobs-history']       = 604800
}

if command == 'get' then
	if ARGV[2] then
		return redis.call('hget', 'ql:config', ARGV[2]) or defaults[ARGV[2]]
	else
		-- Inspired by redis-lua https://github.com/nrk/redis-lua/blob/version-2.0/src/redis.lua
		local reply = redis.call('hgetall', 'ql:config')
	    for i = 1, #reply, 2 do defaults[reply[i]] = reply[i + 1] end
		return cjson.encode(defaults)
	end
elseif command == 'set' then
	local option = assert(ARGV[2], 'Config(): Arg "option" missing')
	local value  = assert(ARGV[3], 'Config(): Arg "value" missing')
	redis.call('hset', 'ql:config', option, value)
elseif command == 'unset' then
	local option = assert(ARGV[2], 'Config(): Arg "option" missing')
	redis.call('hdel', 'ql:config', option)
else
	error('Config(): Unrecognized command ' .. command)
end


