API
===
Here is some brief documentation of how the lua scripts work and what they 
expect. Each invocation begins with a number, which describes how many of the
provided values are considered `KEYS`, as they refer in some sense to a Redis 
key. The remaining values are considered `ARGV`. This is a distinction that 
Redis makes internally, and should be considered a magic number.

Common Arguments
----------------
All times are specified as UTC timestamps. I imagine this is something that 
might become somewhat contentious. Lua scripts are not allowed access to the 
system clock. They actually have a pretty good reason for that, but it also 
means that each client must provide their own times. This has the side effect 
of requiring that all clients have relatively __synchronized clocks__.

- `id` -- the id of the job, a hexadecimal uuid
- `data` -- a JSON blob representing the user data associated with a job
- `queue` -- the name of a queue
- `worker` -- a unique string identifying a process on a host

Cancel(0, id)
-------------
Cancel a job from taking place. It will be deleted from the system, and any
attempts to renew a heartbeat will fail, and any attempts to complete it
will fail. If you try to get the data on the object, you will get nothing.

Complete(0, jid, worker, queue, now, data, ['next', n, [('delay', d) | ('depends', '["jid1","jid2",...]')])
------------------------------------------------------------
Complete a job and optionally put it in another queue, either scheduled or to
be considered waiting immediately. Job dependencies can also be injected at
this time. __Returns__: The updated state, or False on error

Depends(0, jid, ('on', [jid, [jid, [...]]]) | ('off', ('all' | [jid, [jid, [...]]]))
-------------------------------------------------------------------------------
Add or remove dependencies a job has. If 'on' is provided, the provided jids
are added as dependencies. If 'off' and 'all' are provided, then all the 
current dependencies are removed. If 'off' is provided and the next argument 
is not 'all', then those jids are removed as dependencies.

If a job is not already in the 'depends' state, then this call will return 
false. Otherwise, it will return true

Fail(0, id, worker, type, message, now, [data])
-----------------------------------------------
Mark the particular job as failed, with the provided type, and a more specific
message. By `type`, we mean some phrase that might be one of several
categorical modes of failure. The `message` is something more job-specific,
like perhaps a traceback.

This method should __not__ be used to note that a job has been dropped or has 
failed in a transient way. This method __should__ be used to note that a job
has something really wrong with it that must be remedied.

The motivation behind the `type` is so that similar errors can be grouped
together. Optionally, updated data can be provided for the job. A job in any 
state can be marked as failed. If it has been given to a worker as a job, then
its subsequent requests to heartbeat or complete that job will fail. Failed
jobs are kept until they are canceled or completed. __Returns__ the id of the
failed job if successful, or `False` on failure.

Failed(0, [type, [start, [limit]]])
-----------------------------------
If no type is provided, this returns a JSON blob of the counts of the various
types of failures known. If a type is provided, it will report up to `limit`
from `start` of the jobs affected by that issue. __Returns__ a JSON blob.

	# If no type, then...
	{
		'type1': 1,
		'type2': 5,
		...
	}
	
	# If a type is provided, then...
	{
		'total': 100,
		'jobs': [
			{
				# All the normal keys for a job
				'jid': ...,
				'data': ...
				# The message for this particular instance
				'message': ...,
				'type': ...,
			}, ...
		]
	}

Get(0, id)
----------
Get the data associated with a job. __Returns__: JSON blob describing the
job.

GetConfig(0, [option])
----------------------
Get the current configuration value for that option, or if option is omitted,
then get all the configuration values. __Returns__: The value of the option

Heartbeat(0, id, worker, now, [data])
-------------------------------------
Renew the heartbeat, if possible, and optionally update the job's user data.
__Returns__: a JSON blob with `False` if the job was not renewed, or the
updated expiration time

Jobs(0, ('stalled' | 'running' | 'scheduled' | 'depends'), now, queue)
----------------------------------------------------------
Return all the job ids currently considered to be in the provided state
in a particular queue. The response is a list of job ids:

	[
		jid1, 
		jid2,
		...
	]

Peek(1, queue, count, now)
--------------------------
Similar to the `Pop` command, except that it merely peeks at the next items
in the queue.

Pop(1, queue, worker, count, now)
---------------------------------
Passing in the queue from which to pull items, the current time, when the locks
for these returned items should expire, and the number of items to be popped
off.

Priority(0, jid, priority)
--------------------------
Accepts a jid, and a new priority for the job. If the job doesn't exist, then
return false. Otherwise, return the updated priority. If the job is waiting,
then the change will be reflected in the order in which it's popped

Put(1, queue, jid, klass, data, now, delay, [priority, p], [tags, t], [retries, r], [depends, '[...]'])
--------------------------------------------------------------------------
Either create a new job in the provided queue with the provided attributes,
or move that job into that queue. If the job is being serviced by a worker,
subsequent attempts by that worker to either `heartbeat` or `complete` the
job should fail and return `false`.

The `priority` argument should be negative to be run sooner rather than 
later, and positive if it's less important. The `tags` argument should be
a JSON array of the tags associated with the instance and the `delay`
argument should be in how many seconds the instance should be considered 
actionable. The `retries` argument describes the maximum number of retries
that should be permitted for a job before it is considered automatically
failed.

The `depends` argument is an optional JSON array of job ids on which this
job depends. See the section on dependency for more.

__Returns__: The id of the put job, or raises an error on failure

Queues(0, now, [queue])
-----------------------
Return all the queues we know about, with how many jobs are scheduled, waiting,
and running in that queue. If a queue name is provided, then only the
appropriate response hash should be returned. The response is JSON:

	[
		{
			'name': 'testing',
			'stalled': 2,
			'waiting': 5,
			'running': 5,
			'scheduled': 10
		}, {
			...
		}
	]

Retry(0, jid, queue, worker, now, [delay])
------------------------------------------
This script accepts jid, queue, worker and delay for retrying a job. This is
similar in functionality to `put`, except that this counts against the retries 
a job has for a stage.

If the worker is not the worker with a lock on the job, then it returns false.
If the job is not actually running, then it returns false. Otherwise, it
returns the number of retries remaining. If the allowed retries have been
exhausted, then it is automatically failed, and a negative number is returned.

SetConfig(0, option, [value])
-----------------------------
Set the configuration value for the provided option. If `value` is omitted,
then it will remove that configuration option. __Returns__: nothing

Stats(0, queue, date)
---------------------
Return the current statistics for a given queue on a given date. The results 
are returned are a JSON blob:

	{
		'failed': 3,
		'retries': 5,
		'wait' : {
			'total'    : ...,
			'mean'     : ...,
			'variance' : ...,
			'histogram': [
				...
			]
		}, 'run': {
			'total'    : ...,
			'mean'     : ...,
			'variance' : ...,
			'histogram': [
				...
			]
		}
	}

The histogram's data points are at the second resolution for the first minute,
the minute resolution for the first hour, the 15-minute resolution for the
first day, the hour resolution for the first 3 days, and then at the day
resolution from there on out. The `histogram` key is a list of those values.

Tag(0, (('add' | 'remove'), jid, now, tag, [tag, ...]) | 'get', tag, [offset, [count]])
----------------------------------------------------------------------------------
Accepts a jid, 'add' or 'remove', and then a list of tags to either add or
remove from the job. Alternatively, 'get', a tag to get jobs associated with
that tag, and offset and count.

If 'add' or 'remove', the response is a list of the jobs current tags, or False
if the job doesn't exist. If 'get', the response is of the form:

	{
		total: ...,
		jobs: [
			jid,
			...
		]
	}

Track(0) | Track(0, 'track', jid, now, tag, ...) | Track(0, 'untrack', jid, now)
-------------------------------------------------------------------------------
If no arguments are provided, it returns details of all currently-tracked jobs.
If the first argument is 'track', then it will start tracking the job
associated with that id, and 'untrack' stops tracking it. In this context,
tracking is nothing more than saving the job to a list of jobs that are
considered special.
__Returns__ JSON:

	{
		'jobs': [
			{
				'jid': ...,
				# All the other details you'd get from 'get'
			}, {
				...
			}
		], 'expired': [
			# These are all the jids that are completed and whose data expired
			'deadbeef',
			...,
			...,
		]
	}

Workers(0, now, [worker])
-------------------------
Provide data about all the workers, or if a specific worker is provided, then
which jobs that worker is responsible for. If no worker is provided, expect a
response of the form:

	[
		# This is sorted by the recency of activity from that worker
		{
			'name'   : 'hostname1-pid1',
			'jobs'   : 20,
			'stalled': 0
		}, {
			...
		}
	]

If a worker id is provided, then expect a response of the form:

	{
		'jobs': [
			jid1,
			jid2,
			...
		], 'stalled': [
			jid1,
			...
		]
	}

Unfail(0, now, group, queue, [count])
-------------------------------------
Move the first `count` jobs from failure `group` to `queue`. This is
significantly faster than moving each of the jobs individually, so when moving
lots of failed jobs, use this.

ConsistencyCheck(0, [resolve])
------------------------------
__Unimplemented__ This is something I may implement at some point to 
serve as a method for checking the consistency of qless.

Features and Philosophy
=======================

Locking
-------
A worker is given an exclusive lock on a piece of work when it is given
that piece of work. That lock may be renewed periodically so long as it's
before the provided 'heartbeat' timestamp. Likewise, it may be completed.

If a worker attempts to heartbeat a job, it may optionally provide an updated
JSON blob to describe the job. If the job has been given to another worker,
the heartbeat should return `false` and the worker should yield.

When a node attempts to heartbeat, the lua script should check to see if the
node attempting to renew the lock is the same node that currently owns the
lock. If so, then the lock's expiration should be pushed back accordingly, 
and the updated expiration returned. If not, it only has to return false.

Stats
-----
Qless also collects statistics for job wait time (time popped - time put),
and job completion time (time completed - time popped). By 'statistics',
I mean average, variange, count and a histogram. Stats for the number of
failures and retries for a given queue are also available.

Stats are grouped by day. In the case of job wait time, its stats are 
aggregated on the day when the job was popped. In the case of completion time,
they are grouped by the day it was completed.

Tracking
--------
Jobs can be tracked, which just means that they are accessible and displayable.
This can be useful if you just want to keep tabs on the progress of jobs
through the pipeline. All the currently-tracked jobs are stored in a sorted
set, `ql:tracked`.

Failures
--------
Failures are stored in such a way that we can quickly summarize the number of
failures of a given type, but also which items have succumb to that type of
failure. With that in mind, there is a Redis set, `ql:failures` whose members
are the names of the various failure lists. Each type of failure then has its
own list of instance ids that encountered such a failure. For example, we
might have:

	ql:failures
	=============
	upload error
	widget failure
	
	ql:f:upload error
	==================
	deadbeef
	...

Worker Data
-----------
We'll keep a sorted set of workers sorted by the last time they had any
activity. We'll store this set at `ql:workers`.

In addition to this list, we'll keep a set of the jids that a worker currently
has locks for at `ql:w:<worker>:jobs`. This should be sorted by the time when
we last saw a heartbeat (or pop) for that worker from that job.

__TBD__ We will likely store data about each worker. Perhaps this, too, can
be kept by day.

Job Data Deletion
-----------------
We should delete data about completed jobs periodically. We should prune both
by the policies for the maximum number of retained completed jobs, and by the
maximum age for retained jobs. To accomplish this, we'll use a sorted list to
keep track of which items should be expired. This list should be stored in the
key `ql:completed`


Configuration Options
=====================
The configuration should go in the key `ql:config`, and here are some of the
configuration options that `qless` is meant to support:

1. `heartbeat` (60) --
	The default heartbeat in seconds for queues
1. `stats-history` (30) --
	The number of days to store summary stats
1. `histogram-history` (7) --
	The number of days to store histogram data
1. `jobs-history-count` (50k) --
	How many jobs to keep data for after they're completed
1. `jobs-history` (7 * 24 * 60 * 60) --
	How many seconds to keep jobs after they're completed
1. `heartbeat-<queue name>` --
	The heartbeat interval (in seconds) for a particular queue
1. `max-worker-age` --
    How long before workers are considered disappeared


Internal Redis Structure
========================
This section stands to speak to the internal structure and naming conventions.

Jobs
----
Each job is stored primarily in a key `ql:j:<jid>`, a Redis hash, which
contains most of the keys that describe the job. A set (possibly empty)
of jids on which this job depends is stored in `ql:j:<jid>-dependencies`.
A set (also possibly empty) of jids that rely on the completion of this
job is stored in `ql:j:<jid>-dependents`. For example, `ql:j:<jid>`:

	{
		# This is the same id as identifies it in the key. It should be
		# a hex value of a uuid
		'jid'         : 'deadbeef...',
		
		# This is a 'type' identifier. Clients may choose to ignore it,
		# or use it as a language-specific identifier for determining
		# what code to run. For instance, it might be 'foo.bar.FooJob'
		'type'        : '...',
		
		# This is the priority of the job -- lower means more priority.
		# The default is 0
		'priority'    : 0,
		
		# This is the user data associated with the job. (JSON blob)
		'data'        : '{"hello": "how are you"}',
		
		# A JSON array of tags associated with this job
		'tags'        : '["testing", "experimental"]',
		
		# The worker ID of the worker that owns it. Currently the worker
		# id is <hostname>-<pid>
		'worker'      : 'ec2-...-4925',
		
		# This is the time when it must next check in
		'expires'     : 1352375209,
		
		# The current state of the job: 'waiting', 'pending', 'complete'
		'state'       : 'waiting',
		
		# The queue that it's associated with. 'null' if complete
		'queue'       : 'example',
		
		# The maximum number of retries this job is allowed per queue
		'retries'     : 3,
		# The number of retries remaining
		'remaining'   : 3,
		
		# The jids that depend on this job's completion
		'dependents'  : [...],
		# The jids that this job is dependent upon
		'dependencies': [...],
		
		# A list of all the stages that this node has gone through, and
		# when it was put in that queue, given to a worker, which worker,
		# and when it was completed. (JSON blob)
		'history'   : [
			{
				'q'     : 'test1',
				'put'   : 1352075209,
				'popped': 1352075300,
				'done'  : 1352076000,
				'worker': 'some-hostname-pid'
			}, {
				...
			}
		]
	}

Queues
------
A queue is a priority queue and consists of three parts:

1. `ql:q:<name>-scheduled` -- sorted set of all scheduled job ids
1. `ql:q:<name>-work` -- sorted set (by priority) of all jobs waiting
1. `ql:q:<name>-locks` -- sorted set of job locks and expirations
1. `ql:q:<name>-depends` -- sorted set of jobs in a queue, but waiting on other jobs

When looking for a unit of work, the client should first choose from the 
next expired lock. If none are expired, then we should next make sure that
any jobs that should now be considered eligible (the scheduled time is in
the past) are then inserted into the work queue. A sorted set of all the 
known queues is maintained at `ql:queues`. Currently we're keeping it 
sorted based on the time when we first saw the queue, but that's a little
bit at odd with only keeping queues around while they're being used.

When a job is completed, it removes itself as a dependency of all the jobs
that depend on it. If it was the last job that a job depended on, it is then
inserted into the queue's work.

Stats
-----
Stats are grouped by day and queue. The day portion of the stats key is
an integer timestamp of midnight for that day:

	<day> = time - (time % (24 * 60 * 60))

Stats are stored under two hashes: `ql:s:wait:<day>:<queue>` and 
`ql:s:run:<day>:<queue>` respectively. Each has the keys:

- `total` -- The total number of data points contained
- `mean` -- The current mean value
- `vk` -- Not the actual variance, but a number that can be used to both numerically
	stable-ly find the variance, and compute it in a
	[streaming fashion](http://www.johndcook.com/standard_deviation.html)
- `s1`, `s2`, ..., -- second-resolution histogram counts for the first minute
- `m1`, `m2`, ..., -- minute-resolution for the first hour
- `h1`, `h2`, ..., -- hour-resolution for the first day
- `d1`, `d2`, ..., -- day-resolution for the rest

This is also another hash, `ql:s:stats:<day>:<queue>` with keys:

- `failures` -- This is how many failures there have been. If a job is run
	twice and fails repeatedly, this is incremented twice.
- `failed`   -- This is how many are currently failed
- `retries`  -- This is how many jobs we've had to retry

Tags
----
All jobs store a JSON array of the tags that are associated with it. In
addition, the keys `ql:t:<tag>` store a sorted set of all the jobs associated
with that particular tag. The score of each jid in that tag is the time when
that tag was added to that job. When jobs are tagged a second time with an
existing tag, then it's a no-op.


Notes About Implementing Bindings
=================================
There are a few nuanced aspects of implementing bindings for your particular
language that are worth bringing up.

Timestamps and Job Insertion
----------------------------
Jobs with identical priorities are popped in the order they were inserted. The
caveat is that it's only true to the precision of the timestamps your bindings
provide. For example, if you provide timestamps to the second granularity, then
jobs with the same priority inserted in the same second can be popped in any
order. Timestamps at the thousandths of a second granularity will maintain this
property better. While for most applications it's likely not important, it is
something to be aware of when writing language bindings.

Filesystem Access
-----------------
It's intended to be a common usecase that bindings provide a worker script or 
binary that runs several worker subprocesses. These should run with their 
working directory as a sandbox.

Threading or Forking
--------------------
Resque holds the philosphy that each worker consists of two processes: a master
process that grabs jobs, and an actual worker, which is a fork of the master
process and actually does the work associated with the job. The major advantage
of this as far as we can see it is that it's a good strategy for sandboxing any
havoc that might occur when processing the job.

However, apparently Ruby doesn't handle copy-on-write very well, and so there
is a lot of overhead in not only system calls to fork a process, but also to
load modules into memory. Other languages, however, may or may not suffer from
this problem, but it's important to be aware of.

Another project in the vein of Resque (in fact, it uses the same job structure)
is [sidekiq](https://github.com/mperham/sidekiq), which uses threads in a
master process to do work. The performance boost is substantial, not to
mention the memory footprint. Most of this performance appears to be gained
from the memory profile and not constantly allocating memory for every job.

Ultimately, the choice is yours, but we thought it bore mentioning.

Queue Popping Order
-------------------
Workers are allowed (and encouraged) to pop off of more than one queue. But
then we get into the problem of what order they should be polled. Workers
should support two modes of popping: ordered and round-robin. Consider queues
`A`, `B`, and `C` with job counts:

	A: 5
	B: 2
	C: 3

In an ordered verion, the order in which the queues are specified has
significance in the order in which jobs are popped. For example, if our queued
were ordered `C, B, A` in the worker, we'd pop jobs off:

	C, C, C, B, B, A, A, A, A, A

In the round-robin implementation, a worker pops off a job from each queue as
it progress through all queues:

	C, B, A, C, B, A, C, A, A, A


Internal Style Guide
====================
These aren't meant to be stringent, but just to keep myself sane so that when
moving between different chunks of code that it's all formatted similarly, and
the same variable names have the same meaning.

1. Parameter sanitization should be performed as early as possible. This
	includes making use of `assert` and `error` based on the number and type
	of arguments.
1. Job ids should be referred to as `jid`, both internally and in the clients.
1. Failure types should be described with `group`. I'm not terribly thrilled
	with the term, but I thought it was better than 'kind.' After spending
	some time with a Thesaurus, I didn't find anything that appealed to me more
1. Job types should be described as `klass` (nod to Resque), because both
	'type' and 'class' are commonly used in languages.



