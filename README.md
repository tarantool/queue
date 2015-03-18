# A collection of persistent queue implementations for Tarantool 1.6
[![Build Status](https://travis-ci.org/tarantool/queue.svg?branch=master)](https://travis-ci.org/tarantool/queue)
## `fifo` - a simple queue

Features:

* in case there is no more than one consumer, tasks are scheduled in strict FIFO order
* for many concurrent consumers, FIFO is preserved but is less strict: concurrent consumers may complete tasks in different order; on average, FIFO is preserved
* Available properties of queue object:
 * `temporary` - if true, the queue is in-memory only (the contents does not persist on disk)
 
`fifo` queue does not support:
 * task priorities
 * task time to live (`ttl`), execute (`ttr`), delayed tasks (`delay` option)


## `fifottl` - a simple priority queue with task time to live support

The following options can be supplied when creating a queue:
 * `temporary` - if true, the content of the queue does not persist on disk
 * `ttl` - time to live for a task put into the queue; if `ttl` is not given, it's set to infinity
 * `ttr` - time allotted to the worker to work on a task; if not set, is the same as `ttl`
 * `pri` - task priority (`0` is the highest priority and is the default)
 
When a message (task) is pushed into a `fifottl` queue, the following options can be set:
`ttl`, `ttr`, `pri`, and`delay`

Example:

```lua

queue.tube.tube_name:put('my_task_data', { ttl = 60.1, delay = 80 })

```

In the example above, the task has 60.1 seconds to live, but execution is postponed for 80 secods. Delayed start automatically extends the time to live of a task to the amount of the delay, thus the actual time to live is 140.1 seconds.

The value of priority is **lowest** for the **highest**. In other words, a task with priority 1 is executed after the task with priority 0, all other options being equal.

## `utube` - a queue with micro-queues inside

The main idea of this queue backend is the same as in `fifo` queue: the tasks are executed in FIFO order, there is no `ttl`/`ttr`/`delay`/`priority` support.

The main new feature of this queue is that each `put` call accepts a new option, `utube` - the name of the sub-queue.
The sub-queues split the task stream according to subqueue name: it's not possible to take two tasks
out of a sub-queue concurrently, ech sub-queue is executed in strict FIFO order, one task at a time.

### Example: 

Imagine a web-crawler, fetching and parsing perhaps the entire Internet.
The crawler is based on a queue, each task in the queue being a URL
which needs to be downloaded and processed.
If there are many workers, the same URL may show up in the queue many times
-- since it may be referred to by main linking pages. And the workers,
working in parallel, can DOS this site, resulting in the crawler
ending up in the web server user-agent ban list :)

If the domain name is used as a micro-queue name, thiis problem can be
solved: all URLs of the same domain name can be fetched and processed
in strict FIFO order.

## `utubettl` - extention of `utube` to support `ttl`

This tube works the same way as 'fifottl' and 'utube' queues.

# The supporting schema

This purpose of this part is to give you an idea how the various
queues map to Tarantool data structures: spaces, fibers, IPC channels, etc.

The queue metadata (which queues exist, and their properties) are 
stored in space `_queue`. This space is created automatically upon
first invocation of `init` call, or is used if it already exists.

#### The schema of space `_queue`

1. `tube` - the name of the queue
1. `tube_id` - queue ID, numeric
1. `space` - the name bound to the queue and supporting it (stores queue
   data)
1. `type` - the queue type
1. `opts` - additional options supplied when creating the queue

Two more temporary spaces exist to support all queues:

#### The list of waiting consumers `_queue_consumers`

Waiting consumers can pile up when there are no taks.

1. `session` - session (connection) id of the client
1. `fid` - client fiber id
1. `tube_id` - queue id, the client is waiting for a task in this queue
1. `timeout` - the client wait timeout
1. `time` - when the client has come for a task 

#### The list of in progress tasks `_queue_taken`

1. `session` - session id of the client connection
1. `tube_id` - queue id, to which the task belongs
1. `task_id` - task id (of the task being taken)
1. `time` - task execution start time

# API

## Each task is represented as a tuple with 3 fields:

1. task id - numeric
1. task status  - new, in progress, etc
1. task data (JSON)

Queues with ttl, priority or delay support, obviously, 
store additional task fields.

Task status is one of the following (different queues support use  different
sets of status values, so this is a superset):

* `r` - the task is ready for execution (the first `consumer` executing
`take` will get it) 
* `t` - the task has been taken by a consumer
* `-` - the task is executed (a task is pruned from the queue when it's
  executed, so this status may be hard to see)
* `!` - the task is buried (disabled temporarily until further changes)
* `~` - the task is delayed for some time

Task ID is assigned to a task when it's inserted into a queue. Currently task ids are
simple integers for `fifo` и `fifottl` queues.

## Using a queue module

```lua
    local queue = require 'queue'
```

When invoking `require`, a supporting space `_queue` is created (unless
it already exists). The same call sets all the necessary space triggers,
etc.

## Creating a new queue

```
    queue.create_tube(name, type, { ... })
```

Creates a queue.

The following queue types are supported:

1. `fifo` - tasks are executed in FIFO order, unless concurrent
consumers slightly "bias" FIFO since they execute tasks at
different speeds or fail.

1. `fifottl` - a FIFO queue which supports task time to live (`ttl`)
and time to run (`ttr`).
The queue constructor can be given defaults for `ttr` and `ttl`, and
each task in its options can supersede the defaults. The default defaults
are infinity.
1. `utube` - a queue of micro-queues, or partitioned queue
1. `utubettl` - a queue of micro-queues with `ttl`, `ttr` and so on

## Producer API

To insert a new task into the queue, use:

```lua
queue.tube.tube_name:put(task_data[, opts])
```

Options `opts` are optional, and if they are not provided, the defaults
provided in the queue constructor are used. When a queue doesn't have
default options set, the default defaults are used (infinity for `ttl` and
`ttr`, zero for `delay`).

The full list of options is (simpler queues may not support some of them):
опции):

* `ttl` - task time to live in seconds. If a task is not taken by any consumer during its time to live, it's removed from the queue.
* `ttr` - time allotted to execute a task. If a consumer can't run the task
  with the given time limit, the task is reset to `READY` state,
  The `READY` task can be taken by any other consumer. By default, `ttr`
  equals `ttl`.
* `pri` - task priority, **lowest** is the **highest**
* `delay` - task execution must be delayed for the given number of seconds.
  Delay time is added to the total time to live of the task.

This method returns the created task.

## Consumer API

Get a task for execution:

```lua
queue.tube.tube_name:take([timeout])
```

Waits `timeout` seconds until a `READY` task appears in the queue.
Returns either a task object or nil.

The consumer signals successful task execution with `ack` method:

```lua
queue.tube.tube_name:ack(task_id)
```

Please note:

1. `ack` is accepted only from the consumer, which took the task for
execution
1. if a consumer disconnects, all tasks taken by this consumer are put back
to `READY` state (in other words, the tasks are `release`d).

If a consumer for any reason can not execute a task, it can put the
task back into the queue:

```lua
queue.tube.tube_name:release(task_id, opts)
```

the options may contain a possible new `delay` before the task is executed
again.

## Miscellaneous

To look at a task without changing its state, use:

```lua

local task = queue.tube.tube_name:peek(task_id)

```

If a worker suddenly realized that a task is somehow poisoned, can
not be executed in the current circumstances, it can **bury** it,
in other words, disable it until its restored again:

```lua

queue.tube.tube_name:bury(task_id)

```

To reset back to `READY` a bunch of buried task one can use `kick`:

```lua

queue.tube.tube_name:kick(count)

```

A task (in any state) can be deleted permanently with `delete`:

```lua

queue.tube.tube_name:delete(task_id)

```


The entire queue can be dropped (if there are no in-progress tasks or 
workers) with `drop`:

```lua

queue.tube.tube_name:drop()

```

# Implementation details

The implementation is based on the common functions for all queues:

1. controlling the `consumers`' (watching connection state/wakeup)
1. similarities of the API
1. spaces to support each tube
1. etc

Each new queue has a "driver" to support it. 

## Queue drivers

Mandatory requirements

1. The driver works with tuples. The only thing the driver needs
to know about the tuples is their first two fields: id and state.
1. Whenever the driver notices that a task state has changed, it must
notify the framework about the change. 
1. The driver must not throw execptions, unless the driver API is misused.
I.e. for normal operation, even errors during normal operation, there
should be no exceptions. 

## Driver API

Driver class must implement the following API:

1. `new` (constructs an instance of a driver), takes:
 * the space object, in which the driver must store its tasks
 * a callback to notify the main queue framework on a task state change:
 (`on_task_change`)
 * options of the queue (a Lua table)
1. `create_space` - creates the supporting space. The arguments are:
 * space name
 * space options

To sum up, when the user creates a new queue, the queue framework
passes the request to the driver, asking it to create a space to 
support this queue, and then creates a driver instance, passing to it
the created space object.

The same call sequence is used when the queue is "restarted" after
Tarantool server restart.

The driver instance returned by `new` method must provide the following
API:

* `tube:normalize_task(task)` - converts the task tuple to the object 
which is passed on to the user (removes the administrative fields)
* `tube:put(data[, opts])` - puts a task into the queue. 
Returns a normalized task which represents a tuple in the space
* `tube:take()` - sets task state to 'in progress' and returns the task.
If there are no `READY` tasks in the queue, returns nil.
* `tube:delete(task_id)` - deletes a task from the queue
* `tube:release(task_id, opts)` - puts a task back to teh queue (in `READY`
  state).
* `tube:bury(task_id)` - buries a task
* `tube:kick(count)` - digs out `count` tasks
* `tube:peek(task_id)` - return task state by ID

For Tarantool 1.5 Queue see [stable branch](https://github.com/tarantool/queue/tree/stable/)
