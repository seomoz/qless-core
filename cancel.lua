-- Cancel(0, jid)
-- --------------
-- Cancel a job from taking place. It will be deleted from the system, and any
-- attempts to renew a heartbeat will fail, and any attempts to complete it
-- will fail. If you try to get the data on the object, you will get nothing.
--
-- Args:
--    1) jid

if #KEYS > 0 then error('Cancel(): No Keys should be provided') end

local function cancel(jid, jid_set)
  if not jid_set[jid] then
    error('Cancel(): ' .. jid .. ' is a dependency of one of the jobs but is not in the provided jid set')
  end

  -- Find any stage it's associated with and remove its from that stage
  local state, queue, failure, worker = unpack(redis.call('hmget', 'ql:j:' .. jid, 'state', 'queue', 'failure', 'worker'))

  if state == 'complete' then
    return false
  else
    -- If this job has dependents, then we should probably fail
    local dependents = redis.call('smembers', 'ql:j:' .. jid .. '-dependents')
    for _, dependent_jid in ipairs(dependents) do
      cancel(dependent_jid, jid_set)
    end

    -- Remove this job from whatever worker has it, if any
    if worker then
      redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)
    end

    -- Remove it from that queue
    if queue then
      redis.call('zrem', 'ql:q:' .. queue .. '-work', jid)
      redis.call('zrem', 'ql:q:' .. queue .. '-locks', jid)
      redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', jid)
      redis.call('zrem', 'ql:q:' .. queue .. '-depends', jid)
    end

    -- We should probably go through all our dependencies and remove ourselves
    -- from the list of dependents
    for i, j in ipairs(redis.call('smembers', 'ql:j:' .. jid .. '-dependencies')) do
      redis.call('srem', 'ql:j:' .. j .. '-dependents', jid)
    end

    -- Delete any notion of dependencies it has
    redis.call('del', 'ql:j:' .. jid .. '-dependencies')

    -- If we're in the failed state, remove all of our data
    if state == 'failed' then
      failure = cjson.decode(failure)
      -- We need to make this remove it from the failed queues
      redis.call('lrem', 'ql:f:' .. failure.group, 0, jid)
      if redis.call('llen', 'ql:f:' .. failure.group) == 0 then
        redis.call('srem', 'ql:failures', failure.group)
      end
    end

    -- Remove it as a job that's tagged with this particular tag
    local tags = cjson.decode(redis.call('hget', 'ql:j:' .. jid, 'tags') or '{}')
    for i, tag in ipairs(tags) do
      redis.call('zrem', 'ql:t:' .. tag, jid)
      redis.call('zincrby', 'ql:tags', -1, tag)
    end

    -- If the job was being tracked, we should notify
    if redis.call('zscore', 'ql:tracked', jid) ~= false then
      redis.call('publish', 'canceled', jid)
    end

    -- Just go ahead and delete our data
    redis.call('del', 'ql:j:' .. jid)
  end
end

-- Taken from: http://www.lua.org/pil/11.5.html
local function to_set(list)
  local set = {}
  for _, l in ipairs(list) do set[l] = true end
  return set
end

local jids    = assert(ARGV, 'Cancel(): Arg "jid" missing.')
local jid_set = to_set(jids)

for _, jid in ipairs(jids) do
  cancel(jid, jid_set)
end

