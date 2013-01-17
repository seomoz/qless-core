-- DeregisterWorkers(0, worker)
-- This script takes the name of a worker(s) on removes it/them 
-- from the ql:workers set.
--
-- Args: The list of workers to deregister.

if #KEYS > 0 then error('DeregisterWorkers(): No Keys should be provided') end
if #ARGV < 1 then error('DeregisterWorkers(): Must provide at least one worker to deregister') end

local key = 'ql:workers'

redis.call('zrem', key, unpack(ARGV))
