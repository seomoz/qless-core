-- This script takes the name of the queue(s) and removes it
-- from the ql:paused_queues set.
--
-- Args: The list of queues to pause.

if #KEYS > 0 then error('Pause(): No Keys should be provided') end
if #ARGV < 1 then error('Pause(): Must provide at least one queue to pause') end

local key = 'ql:paused_queues'

redis.call('srem', key, unpack(ARGV))

