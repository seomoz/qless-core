-- Recur(0, 'on', queue, jid, klass, data, now, 'interval', second, offset, [priority p], [tags t], [retries r])
-- Recur(0, 'off', jid)
-- Recur(0, 'get', jid)
-- Recur(0, 'update', jid, ['priority', priority], ['interval', interval], ['retries', retries], ['data', data], ['klass', klass], ['queue', queue])
-- Recur(0, 'tag', jid, tag, [tag, [...]])
-- Recur(0, 'untag', jid, tag, [tag, [...]])
-- -------------------------------------------------------------------------------------------------------
-- This script takes the name of a queue, and then the info
-- info about the work item, and makes sure that jobs matching
-- its criteria are regularly made available.

if #KEYS ~= 0 then
	error('Recur(): Got ' .. #KEYS .. ', expected 0 KEYS arguments')
end

local command  = assert(ARGV[1]               , 'Recur(): Missing first argument')

if command == 'on' then
	local queue    = assert(ARGV[2]               , 'Recur(): Arg "queue" missing')
	local jid      = assert(ARGV[3]               , 'Recur(): Arg "jid" missing')
	local klass    = assert(ARGV[4]               , 'Recur(): Arg "klass" missing')
	local data     = assert(cjson.decode(ARGV[5]) , 'Recur(): Arg "data" missing or not JSON: '    .. tostring(ARGV[5]))
	local now      = assert(tonumber(ARGV[6])     , 'Recur(): Arg "now" missing or not a number: ' .. tostring(ARGV[6]))
	local spec     = assert(ARGV[7]               , 'Recur(): Arg "schedule type" missing')
	if spec == 'interval' then
		local interval = assert(tonumber(ARGV[8]) , 'Recur(): Arg "interval" must be a number: '   .. tostring(ARGV[8]))
		local offset   = assert(tonumber(ARGV[9]) , 'Recur(): Arg "offset" must be a number: '     .. tostring(ARGV[9]))
		if interval <= 0 then
			error('Recur(): Arg "interval" must be greater than or equal to 0')
		end
		-- Read in all the optional parameters
		local options = {}
		for i = 10, #ARGV, 2 do options[ARGV[i]] = ARGV[i + 1] end
		options.tags     = assert(cjson.decode(options.tags or '{}'), 'Recur(): Arg "tags" must be JSON-encoded array of string. Got: ' .. tostring(options.tags))
		options.priority = assert(tonumber(options.priority or 0) , 'Recur(): Arg "priority" must be a number. Got: ' .. tostring(options.priority))
		options.retries  = assert(tonumber(options.retries  or 0) , 'Recur(): Arg "retries" must be a number. Got: ' .. tostring(options.retries))

		local count, old_queue = unpack(redis.call('hmget', 'ql:r:' .. jid, 'count', 'queue'))
		count = count or 0

		-- If it has previously been in another queue, then we should remove 
		-- some information about it
		if old_queue then
			redis.call('zrem', 'ql:q:' .. old_queue .. '-recur', jid)
		end
		
		-- Do some insertions
		redis.call('hmset', 'ql:r:' .. jid,
			'jid'     , jid,
			'klass'   , klass,
			'data'    , cjson.encode(data),
			'priority', options.priority,
			'tags'    , cjson.encode(options.tags or {}),
			'state'   , 'recur',
			'queue'   , queue,
			'type'    , 'interval',
			-- How many jobs we've spawned from this
			'count'   , count,
			'interval', interval,
			'retries' , options.retries)
		-- Now, we should schedule the next run of the job
		redis.call('zadd', 'ql:q:' .. queue .. '-recur', now + offset, jid)
		
		-- Lastly, we're going to make sure that this item is in the
		-- set of known queues. We should keep this sorted by the 
		-- order in which we saw each of these queues
		if redis.call('zscore', 'ql:queues', queue) == false then
			redis.call('zadd', 'ql:queues', now, queue)
		end
		
		return jid
	else
		error('Recur(): schedule type "' .. tostring(spec) .. '" unknown')
	end
elseif command == 'off' then
	local jid = assert(ARGV[2], 'Recur(): Arg "jid" missing')
	-- First, find out what queue it was attached to
	local queue = redis.call('hget', 'ql:r:' .. jid, 'queue')
	if queue then
		-- Now, delete it from the queue it was attached to, and delete the thing itself
		redis.call('zrem', 'ql:q:' .. queue .. '-recur', jid)
		redis.call('del', 'ql:r:' .. jid)
		return true
	else
		return true
	end
elseif command == 'get' then
	local jid = assert(ARGV[2], 'Recur(): Arg "jid" missing')
	local job = redis.call(
	    'hmget', 'ql:r:' .. jid, 'jid', 'klass', 'state', 'queue',
		'priority', 'interval', 'retries', 'count', 'data', 'tags')
	
	if not job[1] then
		return false
	end
	
	return cjson.encode({
	    jid          = job[1],
		klass        = job[2],
	    state        = job[3],
	    queue        = job[4],
		priority     = tonumber(job[5]),
		interval     = tonumber(job[6]),
		retries      = tonumber(job[7]),
		count        = tonumber(job[8]),
		data         = cjson.decode(job[9]),
		tags         = cjson.decode(job[10])
	})
elseif command == 'update' then
	local jid = assert(ARGV[2], 'Recur(): Arg "jid" missing')
	local options = {}
	
	-- Make sure that the job exists
	if redis.call('exists', 'ql:r:' .. jid) ~= 0 then
		for i = 3, #ARGV, 2 do
			local key = ARGV[i]
			local value = ARGV[i+1]
			if key == 'priority' or key == 'interval' or key == 'retries' then
				value = assert(tonumber(value), 'Recur(): Arg "' .. key .. '" must be a number: ' .. tostring(value))
				-- If the command is 'interval', then we need to update the time
				-- when it should next be scheduled
				if key == 'interval' then
					local queue, interval = unpack(redis.call('hmget', 'ql:r:' .. jid, 'queue', 'interval'))
					redis.call('zincrby', 'ql:q:' .. queue .. '-recur', value - tonumber(interval), jid)
				end
				redis.call('hset', 'ql:r:' .. jid, key, value)
			elseif key == 'data' then
				value = assert(cjson.decode(value), 'Recur(): Arg "data" is not JSON-encoded: ' .. tostring(value))
				redis.call('hset', 'ql:r:' .. jid, 'data', cjson.encode(value))
			elseif key == 'klass' then
				redis.call('hset', 'ql:r:' .. jid, 'klass', value)
			elseif key == 'queue' then
				local queue = redis.call('hget', 'ql:r:' .. jid, 'queue')
				local score = redis.call('zscore', 'ql:q:' .. queue .. '-recur', jid)
				redis.call('zrem', 'ql:q:' .. queue .. '-recur', jid)
				redis.call('zadd', 'ql:q:' .. value .. '-recur', score, jid)
				redis.call('hset', 'ql:r:' .. jid, 'queue', value)
			else
				error('Recur(): Unrecognized option "' .. key .. '"')
			end
		end
		return true
	else
		return false
	end
elseif command == 'tag' then
	local jid = assert(ARGV[2], 'Recur(): Arg "jid" missing')
	local tags = redis.call('hget', 'ql:r:' .. jid, 'tags')
	-- If the job has been canceled / deleted, then return false
	if tags then
		-- Decode the json blob, convert to dictionary
		tags = cjson.decode(tags)
		local _tags = {}
		for i,v in ipairs(tags) do _tags[v] = true end
		
		-- Otherwise, add the job to the sorted set with that tags
		for i=3,#ARGV do if _tags[ARGV[i]] == nil then table.insert(tags, ARGV[i]) end end
		
		tags = cjson.encode(tags)
		redis.call('hset', 'ql:r:' .. jid, 'tags', tags)
		return tags
	else
		return false
	end
elseif command == 'untag' then
	local jid  = assert(ARGV[2], 'Recur(): Arg "jid" missing')
	-- Get the existing tags
	local tags = redis.call('hget', 'ql:r:' .. jid, 'tags')
	-- If the job has been canceled / deleted, then return false
	if tags then
		-- Decode the json blob, convert to dictionary
		tags = cjson.decode(tags)
		local _tags    = {}
		-- Make a hash
		for i,v in ipairs(tags) do _tags[v] = true end
		-- Delete these from the hash
		for i = 3,#ARGV do _tags[ARGV[i]] = nil end
		-- Back into a list
		local results = {}
		for i, tag in ipairs(tags) do if _tags[tag] then table.insert(results, tag) end end
		-- json encode them, set, and return
		tags = cjson.encode(results)
		redis.call('hset', 'ql:r:' .. jid, 'tags', tags)
		return tags
	else
		return false
	end
else
	error('Recur(): First argument must be one of [on, off, get, update, tag, untag]. Got ' .. tostring(ARGV[1]))
end
