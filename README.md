# Queue API description

A single Tarantool instance can serve any number of queues, along
with any other database work necessary.

A single properly configured Tarantool/Box space can store any
number of queues. Multiple spaces can be used as well - for
partitioning or logical separation reasons.

Queues support task priority. Priority value lays in the range [0,
255], with default value being 127. A higher value means higher
priority, lower value - lower priority.

Each queue has one (currently) associated *fiber* taking care of
it. The fiber is started upon first access to the queue. The job
of the fiber is to monitor orphaned tasks, as well as prune and
clean the queue from obsolete tasks.

To configure a space supporting queues, use the following parameters:

```cfg
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
                        fieldno = 1,    # queue, aka tube
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
                        fieldno = 3,    # next_event
                        type = "NUM64"
                    }
                ]
            }
        ]
    }
]
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

### Arguments of queue API methods

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
* `timeout` (number) - timeout in seconds for the Queue API method.

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

Queue API methods, such as `put`, `take`, return a task.
The task consists of the following fields:

1. `id` (string) - task identifier
1. `tube` (string) - queue identifier 
1. `status` (string) - task status
1. task data (all fields passed into `put`/`urgent` when
   the task was created)

## API

### Producer

#### queue.put(space, tube, delay, ttl, ttr, pri, ...)

Enqueue a task. Returns a tuple, representing the resulting
task. The optional list of fields with task data can be empty.

#### queue.urgent(space, tube, delay, ttl, ttr, pri, ...)

Enqueue a task. The task will get the highest priority.
If `delay` is not zero, the method is equivalent to `put`.

### Consumer

#### queue.take(space, tube, timeout)

Wait for a task to appear in the queue, and, as soon as there is
a task, mark it as `taken` and return to the consumer. If there is
a `timeout`, and the task doesn't appear until the timeout
expires, returns nil. If timeout is not given, waits indefinitely.

All the time while the consumer is working on a task, it must keep
the connection to the server open. If a connection is closed while
the consumer is still working on a task, it's put back on the
`ready` list.

#### queue.ack(space, id)

Confirm completion of a task. This method verifies that:

* the task is `taken` and 
* the consumer that is confirming the task is the one which took it

Consumer identity is established using the session identifier. In
other words, the task must be confirmed by the same connection 
which took it. If verification fails, the method returns an
error.

On success, deletes the task from the queue.

#### queue.release(space, id [, delay [, ttl ] ])

Return a task back to the queue: the task is not executed. 
Additionally, a new time to live and re-execution delay can be
provided.

#### queue.requeue(space, id)

Return a task to the queue, the task is not executed. Puts
the task to the end of the queue, so that it's executed only
after all existing tasks in the queue are executed.

#### queue.bury(space, id)

Mark a task as `buried`. This special status excludes 
the task from the active list, until it's `dug up`.
Useful when several attempts to execute a task lead to a failure.
Buried tasks can be monitored by the queue owner, and treated
specially.

#### queue.done(space, id, ...)

Marks a task as complete (`done`), but doesn't delete it. 
Replaces task data with the supplied fields.

### Common functions (neither producer nor consumer specifically).

#### queue.dig(space, id)

Снимает с задачи пометку об исключенности из списка выполняемых.

#### queue.kick(space, tube [, count] )

Снимает с указанного количества задач пометку об исключенности из списка
выполняемых. Если `count` не указан, то обрабатывает не более одной задачи.

#### queue.unbury(space, id)

Синоним `dig`

#### queue.delete(space, id)

Удаляет задачу из очереди.

#### queue.meta(space, id)

Возвращает метаинформацию о задаче. Состоит из следующих полей (по порядку)

1. `id` (С) - идентификатор задачи
1. `tube` (С) - название очереди
1. `status` (С) - статус задачи
1. `event` (time64) - время когда произойдет следующее событие, связанное
с данной задачей (например истечет время `ttl` или `ttr`)
1. `ipri` (С) - внутренний приоритет задачи (пользователь не может им управлять)
1. `pri` (С) - приоритет задачи выставленный при ее помещении в очередь
1. `cid` (Ч) - идентификатор консюмера, котрый взял задачу на выполнение
(только для случая `taken`)
1. `created` (time64) - когда задача была создана в очереди
1. `ttl` (time64) - время жизни задачи
1. `ttr` (time64) - максимальный интервал обработки задачи
1. `cbury` (Ч) - сколько раз задача переводилась в статус `buried`
1. `ctaken` (Ч) - сколько раз задача бралась на исполнение
1. `now` (time64) - временная метка на момент вызова `meta`

#### queue.peek(space, id)

Возвращает задачу по ее идентификатору.

#### queue.statistics()

Возвращает статистику накопленную с момента старта очереди.
Статистика возвращается только по тем очередям в которых были хоть какие-то
запросы с момента старта тарантула.

Статистика возвращается в виде набора строк. Каждая нечетная строка - название
счетчика/параметра. Каждая четная строка - значение.
