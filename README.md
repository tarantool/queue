# Queue API description

Tarantool/Queues support task priority. Priority value lays in the range [0, 255],
with default value being 127. A higher value means higher priority, lower value -
lower priority.
A single properly configured Tarantool/Box space can store any number of queues.

Each queue has one (currently) associated *fiber* taking care of it. The fiber
is started upon first access to the queue. The job of the fiber is to monitor
orphaned tasks, as well as prune and clean the queue from obsolete tasks.

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

It may also be desirable to tune server `readahead` configuration variable
if many consumers re-use the same socket for getting and acknowledging tasks.

The recomended value can be calculated as:

```
  consumers-per-socket * (256 + largest task size)
```

For example, if the largest task size is 256 bytes, and average nubmer of consumers
per socket is 10, the recommended `readahead` value must be at least 51 200 bytes.

## Terminology

* *Consumer* - a process, dequeueing and executing tasks
* *Producer* - a process enqueueing new tasks 

### Arguments of queue API methods

* `space` (number) space id. To avoid confusion and broken statistics, 
 it's necessary to consistently use numbers to identify spaces,
* `tube` (string) - queue name,
* `delay` (number) - a delay between the moment a task is queued and is executed in seconds
* `ttl` (number) - task time to live in seconds. If `delay` is given along with `ttl`, 
 the effective task time to live is increased by the amount of `delay`,
* `ttr` (number) - task time to run, the maximal time given allowed to execute a task,
* `pri` (number) - task priority [0..255],
* `id` (string) - task id,
* `timeout` (number) - timeout in seconds for the Queue API method.

### Task states

* `ready` - a task is ready for execution
* `delayed` - a task is awaiting task `delay` to expire, after which it will become `ready`
* `taken` - a task is taken by a consumer and is being executed
* `done` - a task is complete (but not deleted, since the consumer called `done` rather than `ack`)
* `buried` - a task is neither ready nor taken nor complete, it's excluded (perhaps temporarily)
 from the list of tasks for execution, but not deleted.


### Формат задачи

Многие методы, как `put`, `take` возвращают задачу. Задача возвращается в
следующем виде:

1. `id` (С) - идентификатор задачи
1. `tube` (С) - название очереди
1. `status` (С) - статус задачи
1. далее идут данные связанные с задачей (переданные в `put`/`urgent`/`done`)


## API


### Producer

#### queue.put(space, tube, delay, ttl, ttr, pri, ...)

Метод размещает задачу в очереди. Возвращает итоговую задачу.
Данные могут не использоваться.

#### queue.urgent(space, tube, delay, ttl, ttr, pri, ...)

Метод размещает задачу в очереди. Отличается от `put` тем что размещает задачу
для немедленного выполнения. В случае если указан ненулевой `delay`, то данный
метод полностью эквивалентен `put`.

### Consumer

#### queue.take(space, tube, timeout)

Метод ожидает появление задачи в очереди (если таймаут не указан, то ожидает
до бесконечности). При появлении задачи она помечается как `taken` и
возвращается консюмеру. Все время, пока консюмер выполняет задачу он не должен
разрывать соединение, поскольку в ответ на разрыв соединения задаче будет
возвращен статус `ready` (готова к исполнению).

#### queue.ack(space, id)

Информирует очередь о том что задача выполнена. Может выбрасывать исключения
в случаях:

* задача не помечена как выполняющаяся (`taken`)
* задачу брал другой консюмер (только тот консюмер что брал задачу на
выполнение может выполнять метод `ack` для нее)

Вызов данного метода приводит к удалению задачи из очереди.

#### queue.release(space, id [, delay [, ttl ] ])

Возвращает задачу обратно в очередь неисполненной. Можно указать интервал
на который надо отложить повторное выполнение, а так же новый `ttl`.

#### queue.requeue(space, id)

Возвращает задачу обратно в очередь неисполненной. При этом кладет задачу
в конец очереди с тем чтобы повторно она выполнялась только после тех задач
что уже накопились в очереди.

#### queue.bury(space, id)

Помечает задачу как исключенную из списка выполняемых (например после серии
ошибок по выполнению данной задачи).


#### queue.done(space, id, ...)

Помечает задачу как выполненную (`done`), заменяет задаче данные на указанные.


### Common functions

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
