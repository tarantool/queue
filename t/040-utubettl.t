#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local test = (require('tap')).test()
test:plan(17)

local queue = require('queue')
local state = require('queue.abstract.state')

local qc = require('queue.compat')

local tnt  = require('t.tnt')
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'memtx'

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'utubettl', { engine = engine })
local tube2 = queue.create_tube('test_stat', 'utubettl', { engine = engine })
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'utubettl', 'tube.type')

test:test('Utubettl statistics', function(test)
   test:plan(13)
   tube2:put('stat_0')
   tube2:put('stat_1')
   tube2:put('stat_2')
   tube2:put('stat_3')
   tube2:put('stat_4')
   tube2:put('stat_5', {delay=1000})
   tube2:delete(4)
   tube2:take(.001)
   tube2:release(0)
   tube2:take(.001)
   tube2:ack(0)
   tube2:bury(1)
   tube2:bury(2)
   tube2:kick(1)
   tube2:take(.001)

   local stats = queue.statistics('test_stat')

   -- check tasks statistics
   test:is(stats.tasks.taken, 1, 'tasks.taken')
   test:is(stats.tasks.buried, 1, 'tasks.buried')
   test:is(stats.tasks.ready, 1, 'tasks.ready')
   test:is(stats.tasks.done, 2, 'tasks.done')
   test:is(stats.tasks.delayed, 1, 'tasks.delayed')
   test:is(stats.tasks.total, 4, 'tasks.total')

   -- check function call statistics
   test:is(stats.calls.delete, 1, 'calls.delete')
   test:is(stats.calls.ack, 1, 'calls.ack')
   test:is(stats.calls.take, 3, 'calls.take')
   test:is(stats.calls.kick, 1, 'calls.kick')
   test:is(stats.calls.bury, 2, 'calls.bury')
   test:is(stats.calls.put, 6, 'calls.put')
   test:is(stats.calls.release, 1, 'calls.release')
end)


test:test('Easy put/take/ack', function(test)
    test:plan(12)

    test:ok(tube:put(123, {utube = 1}), 'task was put')
    test:ok(tube:put(345, {utube = 1}), 'task was put')
    local task = tube:take()
    test:ok(task, 'task was taken')
    test:is(task[2], state.TAKEN, 'task status')
    test:is(task[3], 123, 'task.data')
    test:ok(tube:take(.1) == nil, 'second task was not taken (the same tube)')

    task = tube:ack(task[1])
    test:ok(task, 'task was acked')
    test:is(task[2], '-', 'task status')
    test:is(task[3], 123, 'task.data')

    task = tube:take(.1)
    test:ok(task, 'task2 was taken')
    test:is(task[3], 345, 'task.data')
    test:is(task[2], state.TAKEN, 'task.status')
end)

test:test('ttr put/take', function(test)
    test:plan(3)
    local my_queue = queue.create_tube('trr_test', 'utubettl', { engine = engine })
    test:ok(my_queue:put('ttr1', { ttr = 1 }), 'put ttr task')
    test:ok(my_queue:take(0.1) ~= nil, 'take this task')
    fiber.sleep(1)
    local task = my_queue:peek(0)
    test:is(task[2], state.READY, 'Ready state returned after one second')
end)

test:test('ack in utube', function(test)
    test:plan(8)

    test:ok(tube:put(123, {utube = 'abc'}), 'task was put')
    test:ok(tube:put(345, {utube = 'abc'}), 'task was put')

    local state = 0
    fiber.create(function()
        fiber.sleep(0.1)
        local taken = tube:take()
        test:ok(taken, 'second task was taken')
        test:is(taken[3], 345, 'task.data')
        state = state + 1
    end)

    local taken = tube:take(.1)
    state = 1
    test:ok(taken, 'task was taken')
    test:is(taken[3], 123, 'task.data')
    fiber.sleep(0.3)
    test:is(state, 1, 'state was not changed')
    tube:ack(taken[1])
    fiber.sleep(0.2)
    test:is(state, 2, 'state was changed')
end)
test:test('bury in utube', function(test)
    test:plan(8)

    test:ok(tube:put(567, {utube = 'cde'}), 'task was put')
    test:ok(tube:put(789, {utube = 'cde'}), 'task was put')

    local state = 0
    fiber.create(function()
        fiber.sleep(0.1)
        local taken = tube:take()
        test:ok(taken, 'second task was taken')
        test:is(taken[3], 789, 'task.data')
        state = state + 1
    end)

    local taken = tube:take(.1)
    state = 1
    test:ok(taken, 'task was taken')
    test:is(taken[3], 567, 'task.data')
    fiber.sleep(0.3)
    test:is(state, 1, 'state was not changed')
    tube:bury(taken[1])
    fiber.sleep(0.2)
    test:is(state, 2, 'state was changed')
end)
test:test('instant bury', function(test)
    test:plan(1)
    tube:put(1, {ttr=60})
    local taken = tube:take(.1)
    test:is(tube:bury(taken[1])[2], '!', 'task is buried')
end)
test:test('release in utube', function(test)
    test:plan(8)

    test:ok(tube:put(678, {utube = 'def'}), 'task was put')
    test:ok(tube:put(890, {utube = 'def'}), 'task was put')

    local state = 0
    fiber.create(function()
        fiber.sleep(0.1)
        local taken = tube:take()
        test:ok(taken, 'first task was taken again')
        test:is(taken[3], 678, 'task.data')
        state = state + 1
    end)

    local taken = tube:take(.1)
    state = 1
    test:ok(taken, 'task was taken ' .. taken[1])
    test:is(taken[3], 678, 'task.data')
    fiber.sleep(0.3)
    test:is(state, 1, 'state was not changed')
    tube:release(taken[1])
    fiber.sleep(0.2)
    test:is(state, 2, 'state was changed')
end)

test:test('release[delay] in utube', function(test)
    test:plan(8)

    test:ok(tube:put(789, {utube = 'efg'}), 'task was put')
    test:ok(tube:put(901, {utube = 'efg'}), 'task was put')

    local state = 0
    fiber.create(function()
        fiber.sleep(0.1)
        local taken = tube:take()
        test:ok(taken, 'second task was taken')
        test:is(taken[3], 901, 'task.data')
        state = state + 1
    end)

    local taken = tube:take(.1)
    state = 1
    test:ok(taken, 'task was taken ' .. taken[1])
    test:is(taken[3], 789, 'task.data')
    fiber.sleep(0.3)
    test:is(state, 1, 'state was not changed')
    tube:release(taken[1], { delay = 10 })  --
    fiber.sleep(0.2)
    test:is(state, 2, 'state was changed')
end)

test:test('if_not_exists test', function(test)
    test:plan(2)
    local tube = queue.create_tube('test_ine', 'utubettl', {
        if_not_exists = true, engine = engine
    })
    local tube_new = queue.create_tube('test_ine', 'utubettl', {
        if_not_exists = true, engine = engine
    })
    test:is(tube, tube_new, "if_not_exists if tube exists")

    queue.tube['test_ine'] = nil
    local tube_new = queue.create_tube('test_ine', 'utubettl', {
        if_not_exists = true, engine = engine
    })
    test:isnt(tube, tube_new, "if_not_exists if tube doesn't exists")
end)

test:test('read_only test', function(test)
    test:plan(4)
    tube:put('abc', { delay = 0.1 })
    box.cfg{ read_only = true }
    fiber.sleep(0.11)
    if qc.check_version({1, 7}) then
        local task = tube:take(0.2)
        test:isnil(task, "check that task wasn't moved to ready state")
    else
        local stat, task = pcall(tube.take, tube, 0.2)
        test:is(stat, false, "check that task wasn't taken")
    end
    test:is(tube.raw.fiber:status(), 'suspended',
            "check that background fiber isn't dead")
    box.cfg{ read_only = false }
    test:is(tube.raw.fiber:status(), 'suspended',
            "check that background fiber isn't dead again")
    local task = tube:take()
    test:isnt(task, nil, "check that we can take task")
    tube:ack(task[1])
end)

test:test('ttl after delay test', function(test)
    local TTL = 10
    local TTR = 20
    local DELTA = 5
    test:plan(2)
    box.cfg{}
    local tube = queue.create_tube('test_ttl_release', 'utubettl', { if_not_exists = true })
    tube:put({'test_task'}, { ttl = 10, ttr = 20 })
    tube:take()
    tube:release(0, { delay = DELTA })
    local task = box.space.test_ttl_release:get(0)
    test:is(task.ttl, (TTL + DELTA) * 1000000, 'check TTL after release')
    test:is(task.ttr, TTR * 1000000, 'check TTR after release')
end)

test:test('Space of queue is corrupted', function(test)
    test:plan(4)

    local ctubettl = require('queue.abstract.driver.utubettl')
    local space = ctubettl.create_space('corrupted_utubettl_space', { engine = engine })

    local task_id = space.index.task_id
    local status = space.index.status
    local watch = space.index.watch
    local utube = space.index.utube

    test:test('task_id index does not exist', function(test)
        test:plan(2)

        space.index.task_id = nil
        space.index.status = status
        space.index.watch = watch
        space.index.utube = utube

        local q, e = pcall(ctubettl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have task_id index') ~= nil, 'text of exception')
    end)

    test:test('status index does not exist', function(test)
        test:plan(2)

        space.index.task_id = task_id
        space.index.status = nil
        space.index.watch = watch
        space.index.utube = utube

        local q, e = pcall(ctubettl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have status index') ~= nil, 'text of exception')
    end)

    test:test('watch index does not exist', function(test)
        test:plan(2)

        space.index.task_id = task_id
        space.index.status = status
        space.index.watch = nil
        space.index.utube = utube

        local q, e = pcall(ctubettl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have watch index') ~= nil, 'text of exception')
    end)

    test:test('utube index does not exist', function(test)
        test:plan(2)

        space.index.task_id = task_id
        space.index.status = status
        space.index.watch = watch
        space.index.utube = nil

        local q, e = pcall(ctubettl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have utube index') ~= nil, 'text of exception')
    end)
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
