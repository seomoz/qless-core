-- This script takes the name of the queue(s) and adds it
-- to the ql:paused_queues set.
--
-- Args: The list of queues to pause.
--
-- Note: long term, we have discussed adding a rate-limiting
-- feature to qless-core, which would be more flexible and
-- could be used for pausing (i.e. pause = set the rate to 0).
-- For now, this is far simpler, but we should rewrite this
-- in terms of the rate limiting feature if/when that is added.

if #KEYS > 0 then error('Pause(): No Keys should be provided') end
if #ARGV < 1 then error('Pause(): Must provide at least one queue to pause') end

local key = 'ql:paused_queues'

redis.call('sadd', key, unpack(ARGV))

