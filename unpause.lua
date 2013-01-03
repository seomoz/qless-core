-- This script takes the name of the queue and removes it
-- from the ql:paused_queues set.
--
-- Args:
--    1) The queue to unpause.

local key = 'ql:paused_queues'

for index, queue in ipairs(ARGV) do
  redis.call('srem', key, queue)
end

