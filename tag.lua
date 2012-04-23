-- tag(0, (('add' | 'remove'), jid, now, tag, [tag, ...]) | 'get', tag, [offset, [count]])
-- ----------------------------------------------------------------------------------
-- Accepts a jid, 'add' or 'remove', and then a list of tags
-- to either add or remove from the job. Alternatively, 'get',
-- a tag to get jobs associated with that tag, and offset and
-- count
--
-- If 'add' or 'remove', the response is a list of the jobs
-- current tags, or False if the job doesn't exist. If 'get',
-- the response is of the form:
--
--	{
--		total: ...,
--		jobs: [
--			jid,
--			...
--		]
--	}

if #KEYS ~= 0 then
	error('Tag(): Got ' .. #KEYS .. ', expected 0')
end

local command = assert(ARGV[1], 'Tag(): Missing first arg "add", "remove" or "get"')

if command == 'add' then
	local jid  = assert(ARGV[2]          , 'Tag(): Arg "jid" missing')
	local now  = assert(tonumber(ARGV[3]), 'Tag(): Arg "now" is not a number')
	local tags = redis.call('hget', 'ql:j:' .. jid, 'tags')
	-- If the job has been canceled / deleted, then return false
	if tags then
		-- Decode the json blob, convert to dictionary
		tags = cjson.decode(tags)
		local _tags = {}
		for i,v in ipairs(tags) do
			_tags[v] = true
		end
	
		-- Otherwise, add the job to the sorted set with that tags
		for i=4,#ARGV do
			local tag = ARGV[i]
			if _tags[tag] == nil then
				table.insert(tags, tag)
			end
			redis.call('zadd', 'ql:t:' .. tag, now, jid)
		end
	
		tags = cjson.encode(tags)
		redis.call('hset', 'ql:j:' .. jid, 'tags', tags)
		return tags
	else
		return false
	end
elseif command == 'remove' then
	local jid  = assert(ARGV[2]          , 'Tag(): Arg "jid" missing')
	local now  = assert(tonumber(ARGV[3]), 'Tag(): Arg "now" is not a number')
	local tags = redis.call('hget', 'ql:j:' .. jid, 'tags')
	-- If the job has been canceled / deleted, then return false
	if tags then
		-- Decode the json blob, convert to dictionary
		tags = cjson.decode(tags)
		local _tags = {}
		for i,v in ipairs(tags) do
			_tags[v] = true
		end
	
		-- Otherwise, add the job to the sorted set with that tags
		for i=4,#ARGV do
			local tag = ARGV[i]
			_tags[tag] = nil
			redis.call('zrem', 'ql:t:' .. tag, jid)
		end
	
		local results = {}
		for i,tag in ipairs(tags) do
			if _tags[tag] then
				table.insert(results, tag)
			end
		end
	
		tags = cjson.encode(results)
		redis.call('hset', 'ql:j:' .. jid, 'tags', tags)
		return tags
	else
		return false
	end
elseif command == 'get' then
	local tag    = assert(ARGV[2]                , 'Tag(): Arg "tag" missing')
	local offset = assert(tonumber(ARGV[3] or 0) , 'Tag(): Arg "offset" not a number: ' .. tostring(ARGV[3]))
	local count  = assert(tonumber(ARGV[4] or 25), 'Tag(): Arg "count" not a number: ' .. tostring(ARGV[4]))
	return cjson.encode({
		total = redis.call('zcard', 'ql:t:' .. tag),
		jobs  = redis.call('zrange', 'ql:t:' .. tag, offset, count)
	})
else
	error('Tag(): First argument must be "add", "remove" or "get"')
end
