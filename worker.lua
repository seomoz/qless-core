-- Deregisters these workers from the list of known workers
function QlessWorker.deregister(...)
  redis.call('zrem', 'ql:workers', unpack(arg))
end

-- Provide data about all the workers, or if a specific worker is provided,
-- then which jobs that worker is responsible for. If no worker is provided,
-- expect a response of the form:
-- 
--  [
--      # This is sorted by the recency of activity from that worker
--      {
--          'name'   : 'hostname1-pid1',
--          'jobs'   : 20,
--          'stalled': 0
--      }, {
--          ...
--      }
--  ]
-- 
-- If a worker id is provided, then expect a response of the form:
-- 
--  {
--      'jobs': [
--          jid1,
--          jid2,
--          ...
--      ], 'stalled': [
--          jid1,
--          ...
--      ]
--  }
--
function QlessWorker.counts(now, worker)
  -- Clean up all the workers' job lists if they're too old. This is
  -- determined by the `max-worker-age` configuration, defaulting to the
  -- last day. Seems like a 'reasonable' default
  local interval = tonumber(Qless.config.get('max-worker-age', 86400))

  local workers  = redis.call('zrangebyscore', 'ql:workers', 0, now - interval)
  for index, worker in ipairs(workers) do
    redis.call('del', 'ql:w:' .. worker .. ':jobs')
  end

  -- And now remove them from the list of known workers
  redis.call('zremrangebyscore', 'ql:workers', 0, now - interval)

  if worker then
    return {
      jobs    = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now + 8640000, now),
      stalled = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now, 0)
    }
  else
    local response = {}
    local workers = redis.call('zrevrange', 'ql:workers', 0, -1)
    for index, worker in ipairs(workers) do
      table.insert(response, {
        name    = worker,
        jobs    = redis.call('zcount', 'ql:w:' .. worker .. ':jobs', now, now + 8640000),
        stalled = redis.call('zcount', 'ql:w:' .. worker .. ':jobs', 0, now)
      })
    end
    return response
  end
end
