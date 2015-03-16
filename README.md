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
    queue.schema.create_tube(name, type, { ... })
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

Положить таск в очередь можно командой

```lua
queue.tube.tube_name:put(task_data[, opts])
```

Опции `opts` в общем случае необязательны, если не указаны, то берутся из
общих настроек очереди (или устанавливаются в то или иное дефолтное значение).

Общие для всех очередей опции (разные очереди могут не поддерживать те или иные
опции):

* `ttl` - время жизни таска в секундах. Спустя данное время, если таск не был
взят ни одним консьюмером таск удаляется из очереди.
* `ttr` - время отведенное на выполнение таска консьюмером. Если консюмер не
уложился в это время, то таск возвращается в состояние `READY` (и его сможет
взять на выполнение другой консюмер). По умолчанию равно `ttl`.
* `pri` - приоритет задачи.
* `delay` - отложить выполнение задачи на указанное число секунд.

Возвращает созданную задачу.

## Consumer API

Получить таск на выполнение можно командой

```lua
queue.tube.tube_name:take([timeout])
```

Ожидает `timeout` секунд до появления задачи в очереди.
Возвращает задачу или пустой ответ.


После выполнения задачи консюмером необходимо сделать для задачи `ack`:

```lua
queue.tube.tube_name:ack(task_id)
```

Необходимо отметить:

1. `ack` может делать только тот консюмер, который взял задачу на исполнение
1. при разрыве сессии с консюмером, все взятые им задачи переводятся в состояние
"готова к исполнению" (то есть им делается принудительный `release`)

Если консюмер по какой-то причине не может выполнить задачу, то он может
вернуть ее в очередь:

```lua
queue.tube.tube_name:release(task_id, opts)
```

в опциях можно передавать задержку на последующее выполнение (для очередей,
которые поддерживают отложенное исполнение) - `delay`.


## Miscellaneous

Посмотреть на задачу зная ее ID можно испльзуя запрос

```lua

local task = queue.tube.tube_name:peek(task_id)

```

Если воркер понял что с задачей что-то не то и она не может быть выполнена
то он (и не только он) может ее закопать вызвав метод

```lua

queue.tube.tube_name:bury(task_id)

```

Откопать заданное количество проблемных задач можно функцией `kick`:

```lua

queue.tube.tube_name:kick(count)

```

Удалить задачу зная ее ID можно функцией `delete`:

```lua

queue.tube.tube_name:delete(task_id)

```


Удалить очередь целиком (при условии что в ней нет исполняющихся
задач или присоединенных воркеров можно при помощи функции drop:

```lua

queue.tube.tube_name:drop()

```


# Implementation details

Реализация опирается на общие для всех очередей вещи:

1. контроль за `consumer`'ами (ожидание/побудка итп)
1. единообразный API
1. создание спейсов
1. итп

соответственно каждая новая очередь описывается драйвером.

## Драйверы очередей

Основные требования

1. Поддерживает понятия:
 * ненормализованный таск (внутреннее строение таска) - таппл.
   единственное требование: первое поле - ID, второе поле - состояние.
 * нормализованный таск (описан выше)
1. При каждом изменении состояния таска по любой причине (как инициированной
очередью, так и инициированной внутренними нуждами) должен уведомить
основную систему об этом уведомлении.
1. Не выбрасывает исключения вида "задачи нет в очереди" (но может
выбрасывать исключения вида "неверные параметры вызова")

## API драйверов

1. метод `new` (конструктор объекта). принимает два аргумента:
 * спейс (объект) с которым работает данный драйвер
 * функцию для уведомления всей системы о происходящих изменениях
 (`on_task_change`)
 * опции (таблица) очереди
1. метод `create_space` - создание спейса. передаются два аргумента
 * имя спейса
 * опции очереди

Таким образом когда пользователь просит у системы создать новую очередь,
то система просит у драйвера создать спейс для этой очереди, затем
конструирует объект передавая ему созданный спейс.

Объект конструируется так же и в случае когда очередь просто стартует
вместе с запуском тарантула.

Каждый объект очереди имеет следующее API:

* `tube:normalize_task(task)` - должен привести таск к обобщенному виду
  (вырезать из него все ненужные пользователю данные)
* `tube:put(data[, opts])` - положить задачу в очередь. Возвращает
ненормализованный таск (запись в спейсе).
* `tube:take()` - берет задачу готовую к выполнению из очереди (помечая
ее как "выполняющаяся"). Если таких задач нет - возвращает nil
* `tube:delete(task_id)` - удаление задачи из очереди
* `tube:release(task_id, opts)` - возврат задачи в очередь
* `tube:bury(task_id)` - закопать задачу
* `tube:kick(count)` - откопать `count` задач
* `tube:peek(task_id)` - выбрать задачу по ID

For Tarantool 1.5 Queue see [stable branch](https://github.com/tarantool/queue/tree/stable/)
