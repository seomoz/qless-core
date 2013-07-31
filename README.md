Qless Core
==========
This is the set of all the lua scripts that comprise the qless library. We've
begun migrating away from the system of having one lua script per command to
a more object-oriented approach where all code is contained in a single unified
lua script.

There are a few reasons for making this choice, but essentially it was getting
too difficult to maintain and there was a lot of duplicated code in different
sections. This also happens to have the added benefit of allowing you to build
on top of the qless core library __within your own lua scripts__ through
composition.

Building
========
For ease of development, we've broken this unified file into several smaller
submodules that are concatenated together into a `qless.lua` script. These are:

- `base.lua` -- forward declarations and some uncategorized functions
- `config.lua` -- all configuration interactions
- `job.lua` -- the regular job class
- `recurring.lua` -- the recurring job class
- `queue.lua` -- the queue class
- `api.lua` -- exposing the interfaces that the clients invoke, it's a very
	thin wrapper around these classes

In order to build up the `qless.lua` script, we've included a simple `Makefile`
though all it does is cat these files out in a particular order:

```bash
make qless.lua
```

If you'd like to use _just_ the core library within your lua script, you can
get lua script that contains all the classes, but none of the wrapping layer
that the qless clients use:

```bash
make qless-lib.lua
```

Testing
=======
Historically, tests have appeared only in the language-specific bindings of
qless, but that has become a tedious process. Not to mention the fact that
it's a steep barrier to entry for writing new clients. In light of that, we
now include tests directly in `qless-core`, written in python. To run these,
you will need python and the `nose` and `redis` libraries. If you have `pip`
installed:

```python
pip install redis nose
```

To run the tests, there is a directive included in the makefile:

```bash
make test
```

Conventions
===========

No more `KEYS`
--------------
When originally developing this, I wrote some functions using the `KEYS`
portion of the lua scripts, but eventually realized that to do so didn't make
any sense. For just about all operations there's no way to determine a priori
which Redis keys would be touched, and so I abandoned that idea. However, in
many cases there were vestigial `KEYS` in use, but that has now changed. No
more `KEYS`!

Time, Time Everywhere
---------------------
To ease the client logic, every command now takes a timestamp with it. In many
cases this argument is ignored, but it is still required in order to make a
valid call. This requirement only comes through in the exposed script API, but
not in the class interface. At the class function level, only the functions
which require the `now` argument list it.

Documentation
=============
The documentation of the code is present in each of the modules, but it is
excluded from the production code to reduce the weight of it.

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
and the updated expiration returned. If not, an exception is raised.

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
		
		# A list of all the things that have happened to a job. Each entry has
		# the keys 'what' and 'when', but it may also have arbitrary keys
		# associated with it.
		'history'   : [
			{
				'what'  : 'Popped',
				'when'  : 1352075209,
				...
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
1. `ql:q:<name>-depends` -- sorted set of jobs in a queue, but waiting on
    other jobs

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



