-- priority(0, jid, priority)
-- --------------------------
-- Accepts a jid, and a new priority for the job. If the job 
-- doesn't exist, then return false. Otherwise, return the 
-- updated priority. If the job is waiting, then the change
-- will be reflected in the order in which it's popped

if #KEYS ~= 0 then
	error('Priority(): Got ' .. #KEYS .. ', expected 0')
end

local jid      = assert(ARGV[1]          , 'Priority(): Arg "jid" missing')
local priority = assert(tonumber(ARGV[2]), 'Priority(): Arg "priority" missing or not a number: ' .. tostring(ARGV[2]))

-- Get the queue the job is currently in, if any
local queue = redis.call('hget', 'ql:j:' .. jid, 'queue')

if queue == nil then
	return false
elseif queue == false then
	return false
elseif queue == '' then
	-- Just adjust the priority
	redis.call('hset', 'ql:j:' .. jid, 'priority', priority)
	return priority
else
	-- Adjust the priority and see if it's a candidate for updating
	-- its priority in the queue it's currently in
	if redis.call('zscore', 'ql:q:' .. queue .. '-work', jid) then
		redis.call('zadd', 'ql:q:' .. queue .. '-work', priority, jid)
	end
	redis.call('hset', 'ql:j:' .. jid, 'priority', priority)
	return priority
end
