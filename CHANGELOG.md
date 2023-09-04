# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

- In replicaset mode, the behavior of the public API is reduced to the same behavior
  in all queue states, including INIT. Previously, in the INIT state, an ambiguous
  error was thrown when trying to access a public method on a replica and the script
  was interrupted by an error.

  Old behavior (call `create_tube` on replica, queue is in INIT state):
  ```
  2023-09-04 14:01:11.000 [5990] main/103/replica.lua/box.load_cfg I> set 'read_only' configuration option to true
  stack traceback:
    /home/void/tmp/cluster/repl/queue/init.lua:44: in function '__index'
    replica.lua:13: in main chunk
  2023-09-04 14:01:11.004 [5990] main/105/checkpoint_daemon I> scheduled next checkpoint for Mon Sep  4 15:11:32 2023
  2023-09-04 14:01:11.004 [5990] main utils.c:610 E> LuajitError: /home/void/tmp/cluster/repl/queue/init.lua:45: Please configure box.cfg{} in read/write mode first
  ```
  After this fix:
  ```
  2023-09-11 10:24:31.463 [19773] main/103/replica.lua abstract.lua:93 E> create_tube: queue is in INIT state
  ```

## [1.3.2] - 2023-08-24

### Fixed

- Duplicate id error with mvvc on put and take (#207).

## [1.3.1] - 2023-07-31

### Fixed

- Yield in the fifottl/utubettl queue drivers.

## [1.3.0] - 2023-03-13

### Added

- Possibility to get the module version.

### Fixed

- Bug when working with the replicaset (#202).

## [1.2.5] - 2023-02-28

This is a technical release that should fix several problems in the
repositories with packages that were caused by the mistaken creation
of several tags (for this reason there are no 1.2.3 and 1.2.4 tags
and corresponding packages in the repositories).

## [1.2.2] - 2022-11-03

### Fixed

- Excessive CPU consumption of the state fiber.

## [1.2.1] - 2022-09-12

### Added

- Rockspec publishing to CD.

### Fixed

- "replication" mode switching on tarantool < 2.2.1

## [1.2.0] - 2022-06-06

### Added

- Master-replica switching support.
  Added the ability to use a queue in the master replica scheme.
  More information is available:
  https://github.com/tarantool/queue#queue-state-diagram
  https://github.com/tarantool/queue#queue-and-replication
- Granting privilege to `statistics`, `put` and `truncate`.

### Fixed

- Work with "ttl" of buried task.
  Previously, if a task was "buried" after it was "taken" (and the task has a
  "ttr"), then the time when the "release" should occur (the "ttr" timer) will
  be interpreted as the end of the "ttl" timer and the task will be deleted.

## [1.1.0] - 2020-12-25

### Added

- "Shared sessions" was added to the queue. Previously, a connection to server
  was synonym of the queue session. Now the session has a unique UUID (returned
  by the "queue.identify()" method), and one session can have many connections.
  To connect to an existing session, call "queue.identify(uuid)" with the
  previously obtained UUID.
- Possibility to work with tasks after reconnect. The queue module now provides
  the ability to set the `ttr` setting for sessions by
  `queue.cfg({ ttr = ttr_in_sec})`, which characterizes how long the logical
  session will exist after all active connections are closed.

### Fixed

- Custom driver registration after reboot. Previously, if a custom driver is
  registered after calling box.cfg() it causes a problem when the instance will
  be restarted.

## [1.0.8] - 2020-10-17

### Added

- The ability to start an instance with a loaded queue module in read-only
  mode. In this case, a start of the module will be delayed until the instance
  will be configured with read_only = false. Previously, when trying to
  initialize the queue module on an instance configured in ro mode, an error
  occurred: "ER_READONLY: Can't modify data because this instance is in
  read-only mode." See https://github.com/tarantool/queue#initialization for
  more details.

## [1.0.7] - 2020-09-03

### Breaking changes

- A check for a driver API implementation was added. Now, the consumer will be
  informed about the missing methods in the driver implementation (an error will
  be thrown).

### Added

- Notification about missing methods in the driver implementation (#126).

### Fixed

- The tasks releases on start for some drivers (utubettl, fifottl,
  limfifottl). Before, an attempt to release "taken" tasks on start lead to an
  error: "attempt to index local 'opts' (a nil value)" (#121).

### Changed

- Updated the requirements for the "delete" driver method. Now, the method
  should change the state of a task to "done". Before, it was duplicated by
  external code.

## [1.0.6] - 2020-02-29

### Breaking changes

External drivers should be updated with the new `tasks_by_state()` method.
Consider the example from the `fifo` driver:

```lua
-- get iterator to tasks in a certain state
function method.tasks_by_state(self, task_state)
    return self.space.index.status:pairs(task_state)
end
```

The example uses 'status' secondary index, which is built on top of 'status'
and 'task_id' fields.

This new method is necessary to correctly manage state of tasks: when tarantool
instance is restarted we should release all taken tasks (otherwise they would 
stuck in the taken state forever). See #66 and #126 for more information.

### Fixed

- Releasing tasks at a client disconnection (#103).
- Optimize statistics build (-15% in some cases) (#92).
- Release all taken tasks at start (#66).
- Don't take tasks during a client disconnection (#104).
