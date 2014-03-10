function QlessThrottle:acquire(jid)
  if self.available() then
    self.locks.add(jid)
    return true
  else
    queue_obj = Qless.queue(Qless.job(jid).queue)
    queue_obj.throttled.add(jid)
    self.pending.add(jid)
    return false
  end
end

function QlessThrottle:release(now, jid)
  self.locks.remove(jid)
  if self.available() then
    next_jid = self.pending.pop
    if next_jid then
      queue_obj = Qless.queue(Qless.job(next_jid).queue)
      queue_obj.throttled.remove(next_jid)
      queue_obj.work.add(now, next_jid)
    end
  end
end

function QlessThrottle:available()
  return self.maximum == 0 or self.locks.count < self.maximum
end
