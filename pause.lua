-- This script takes the name of the queue and adds it
-- to the ql:paused_queues set.
--
-- Args:
--    1) The queue to pause.
--
-- Note: long term, we have discussed adding a rate-limiting
-- feature to qless-core, which would be more flexible and
-- could be used for pausing (i.e. pause = set the rate to 0).
-- For now, this is far simpler, but we should rewrite this
-- in terms of the rate limiting feature if/when that is added.

local key = 'ql:paused_queues'

for index, queue in ipairs(ARGV) do
  redis.call('sadd', key, queue)
end

