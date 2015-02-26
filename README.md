# Queue API description

A Tarantool/Box instance can serve as a Queue Manager, along
with any other database work necessary.

A single properly configured Tarantool/Box space can store any
number of queues. Multiple spaces can be used as well - for
partitioning or logical separation of queues.

Queues support task priority. Priority value lays in the range
[0, 255], with default value being 127. A higher value means higher
priority, lower value - lower priority.

Each queue has one (currently) associated *fiber* taking care of
it. The fiber is started upon first access to the queue. The job
of the fiber is to monitor orphaned tasks, as well as prune and
clean the queue from obsolete tasks.

To configure a space supporting queues, use the following parameters:

```cfg
readahead   = 16384

primary_port = 33020
secondary_port = 33021
admin_port   = 33022


space = [
    {
        enabled = 1,
        index = [
            {
                type = "TREE",
                unique = 1,
                key_field = [
                    {
                        fieldno = 0,
                        type = "STR"
                    }
                ]
            },
            {
                type = "TREE",
                unique = 0,
                key_field = [
                    {
                        fieldno = 1,    # tube
                        type = "STR"
                    },
                    {
                        fieldno = 2,    # status
                        type = "STR"
                    },
                    {
                        fieldno = 4,    # ipri
                        type = "STR"
                    },
                    {
                        fieldno = 5    # pri
                        type = "STR"
                    }
                ]
            },
            {
                type    = "TREE",
                unique  = 0,
                key_field = [
                    {
                        fieldno = 1,    # tube
                        type = "STR"
                    },
                    {
                        fieldno = 3,    # next_event
                        type = "NUM64"
                    }
                ]
            }
        ]
    }
]
```

If You want use method `queue.put_unique` you have to add additional
(fourth) index:

```cfg
            {
                type    = "TREE",
                unique  = 0,
                key_field = [
                    {
                        fieldno = 1,    # tube
                        type = "STR"
                    },
                    {
                        fieldno = 2,    # status
                        type = "STR"
                    },
                    {
                        fieldno = 12,   # task data
                        type = "STR"
                    }
                ]
            }
```

It may also be desirable to tune server `readahead` configuration
variable if many consumers re-use the same socket for getting and
acknowledging tasks.

The recommended value can be calculated as:

```
  consumers-per-socket * (256 + largest task size)
```

For example, if the largest task size is 256 bytes, and average
number of consumers per socket is 10, the recommended `readahead`
value must be at least 51 200 bytes.

## Terminology

* *Consumer* - a process, taking and executing tasks
* *Producer* - a process adding new tasks

### Arguments of queue API functions

* `space` (number) space id. To avoid confusion and broken statistics,
  it's necessary to consistently use numbers to identify spaces,
* `tube` (string) - queue name,
* `delay` (number) - a delay between the moment a task is queued
  and is executed, in seconds
* `ttl` (number) - task time to live, in seconds. If `delay` is
  given along with `ttl`, the effective task time to live is
  increased by the amount of `delay`,
* `ttr` (number) - task time to run, the maximal time allotted
  to a consumer to execute a task, in seconds,
* `pri` (number) - task priority [0..255],
* `id` (string) - task id,
* `timeout` (number) - timeout in seconds for the Queue API function.

### Task states

* `ready` - a task is ready for execution,
* `delayed` - a task is awaiting task `delay` to expire, after
   which it will become `ready`,
* `taken` - a task is taken by a consumer and is being executed,
* `done` - a task is complete (but not deleted, since the consumer
   called `done` rather than `ack`),
* `buried` - a task is neither ready nor taken nor complete, it's
   excluded (perhaps temporarily) from the list of tasks for
   execution, but not deleted.

### The format of task tuple

Queue API functions, such as `put`, `take`, return a task.
The task consists of the following fields:

1. `id` (string) - task identifier
1. `tube` (string) - queue identifier
1. `status` (string) - task status
1. task data (all fields passed into `put`/`urgent` when
   the task was created)

## API

### Producer

#### queue.put(space, tube, delay, ttl, ttr, pri, ...)

Enqueue a task. Returns a tuple, representing the new task.
The list of fields with task data ('...')is optional.

#### queue.urgent(space, tube, delay, ttl, ttr, pri, ...)

Enqueue a task. The task will get the highest priority.
If `delay` is not zero, the function is equivalent to `put`.

### Consumer

#### queue.take(space, tube, timeout)

If there are tasks in the queue `ready` for execution,
take the highest-priority task.
Otherwise, wait for a `ready` task to appear in the queue, and, as
soon as it appears, mark it as `taken` and return to the consumer.
If there is a `timeout`, and the task doesn't appear until the
timeout expires, return 'nil' (a timeout of 0 returns immediately).
If timeout is not given or negative, wait indefinitely until a task
appears.

All the time while the consumer is working on a task, it must keep
the connection to the server open. If a connection disappears while
the consumer is still working on a task, the task is put back on the
`ready` list.

#### queue.ack(space, id)

Confirm completion of a task. Before marking a task as complete,
this function verifies that:

* the task is `taken` and
* the consumer that is confirming the task is the one which took it.

Consumer identity is established using a session identifier. In
other words, the task must be confirmed by the same connection
which took it. If verification fails, the function returns an
error.

On success, delete the task from the queue.

#### queue.release(space, id [, delay [, ttl ] ])

Return a task back to the queue: the task is not executed.
Additionally, a new time to live and re-execution delay can be
provided.
If `ttl` is not defined (or zero) the method won't prolong ttl.


#### queue.requeue(space, id)

Return a task to the queue, the task is not executed. Puts
the task at the end of the queue, so that it's executed only
after all existing tasks in the queue are executed.

#### queue.bury(space, id)

Mark a task as `buried`. This special status excludes
the task from the active list, until it's `dug up`.
This function is useful when several attempts to execute a task
lead to a failure. Buried tasks can be monitored by the queue
owner, and treated specially.

#### queue.done(space, id, ...)

Mark a task as complete (`done`), but don't delete it.
Replaces task data with the supplied fields ('...').

### Common functions (neither producer nor consumer).

#### queue.dig(space, id)

'Dig up' a buried task, after checking that the task
is buried. The task status is changed to `ready`.

#### queue.kick(space, tube [, count] )

'Dig up' `count` tasks in a queue. If `count` is not given,
digs up just one buried task.

#### queue.unbury(space, id)

An alias to `dig`.

#### queue.delete(space, id)

Delete a task from the queue (regardless of task state or status).

#### queue.truncate(space, tube)

Truncate a tube. Return the number of deleted tasks.

#### queue.meta(space, id)

Return taks metadata:

1. `id` (string) - task id
1. `tube` (string) - queue id
1. `status` (string) - task status
1. `event` (time64) - time of the next important event in task
   life time, for example, when `ttl` or `ttr` expires, in seconds
   since start of the UNIX epoch
1. `ipri` (string) - internal value of the task priority
1. `pri` (string) - task priority as set when the task was added
   to the queue
1. `cid` (number) - consumer id, of the consumer which took the
   task (only if the task is `taken`)
1. `created` (time64) - time when the task was created (seconds
   since start of the UNIX epoch).
1. `ttl` (time64) - task time to live
1. `ttr` (time64) - task time to run
1. `cbury` (count) - how many times the task was buried
1. `ctaken` (Ч) - how many times the task was taken
1. `now` (time64) - time recorded when the meta was called

#### queue.peek(space, id)

Return a task by task id. Returned tuple has the following
fields:

1. `id` (string) - task identifier
1. `tube` (string) - queue identifier
1. `status` (string) - task status
1. task data (all fields passed into `put`/`urgent` when
   the task was created).

#### queue.statistics()

Return queue module statistics accumulated since server start.
The statistics is broken down by queue id. Only queues on which
there was some activity are included in the output.

The format of the statistics is a sequence of rows, where each
odd row is the name of a statistical parameter, and the
next even row is the value.

