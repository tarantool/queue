#!/usr/bin/env tarantool
local fiber = require('fiber')

local test = require('tap').test()
test:plan(16)

local queue = require('queue')
local state = require('queue.abstract.state')

local qc = require('queue.compat')

local tnt  = require('t.tnt')
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'vinyl'

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'fifottl', { engine = engine })
local tube2 = queue.create_tube('test_stat', 'fifottl', { engine = engine })
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'fifottl', 'tube.type')

test:test('Fifottl statistics', function(test)
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


test:test('put/take/peek', function(test)
    test:plan(11)

    local task = tube:put('abc')

    test:ok(task, "task was put")
    test:is(task[2], state.READY, "task.state")

    local peek = tube:peek(task[1])
    test:is_deeply(task[1], peek[1], "put and peek tasks are the same")
    test:is_deeply(task[2], peek[2], "put and peek tasks are the same")
    test:is_deeply(task[3], peek[3], "put and peek tasks are the same")

    local taken = tube:take( .1 )
    test:ok(taken, 'task was taken')

    test:is(task[1], taken[1], 'task.id')
    test:is(taken[2], state.TAKEN, 'task.status')

    local ack = tube:ack(taken[1])
    test:ok(ack, 'task was acked')

    local s, e = pcall(function() tube:peek(task[1]) end)
    test:ok(not s, "peek status")
    test:ok(string.match(e, 'Task %d+ not found') ~= nil, 'peek error message')
end)

test:test('delayed tasks', function(test)
    test:plan(12)

    local task = tube:put('cde', { delay = 0.1, ttl = 0.1, ttr = 0.01 })
    test:ok(task, 'delayed task was put')
    test:is(task[3], 'cde', 'task.data')
    test:is(task[2], state.DELAYED, 'state is DELAYED')

    test:isnil(tube:take(.01), 'delayed task was not taken')

    local taken = tube:take(.15)
    test:ok(taken, 'delayed task was taken after timeout')
    test:is(taken[3], 'cde', 'task.data')

    local retaken = tube:take(0.05)
    test:ok(retaken, "retake task after ttr")
    test:is(retaken[3], 'cde', 'task.data')

    fiber.sleep(0.2)
    local s, e = pcall(function() tube:peek(retaken[1]) end)
    test:ok(not s, 'Task is not in database (TTL)')
    test:ok(string.match(e, 'Task %d+ not found') ~= nil, 'peek error message')


    s, e = pcall(function() tube:ack(retaken[1]) end)
    test:ok(not s, 'Task is not ackable (TTL)')
    test:ok(string.match(e, 'Task was not taken') ~= nil, 'peek error message')
end)

test:test('delete/peek', function(test)
    test:plan(10)

    local task = tube:put('abc')
    test:ok(task, 'task was put')
    test:is(task[2], state.READY, 'task is READY')

    local taken = tube:take(.1)
    test:ok(taken, 'task was taken')
    test:is(taken[3], 'abc', 'task.data')
    test:is(taken[2], state.TAKEN, 'task is really taken')

    local removed = tube:delete(task[1])
    test:ok(removed, 'tube:delete')
    test:is(removed[3], 'abc', 'removed.data')
    test:is(removed[2], state.DONE, 'removed.status')

    local s, e = pcall(function() tube:ack(task[1]) end)
    test:ok(not s, "Can't ack removed task")
    test:ok(string.match(e, 'Task was not taken') ~= nil, 'peek error message')
end)

test:test('bury/peek/kick', function(test)
    test:plan(17)

    local task = tube:put('abc')
    test:ok(task, 'task was put')
    test:is(task[2], state.READY, 'task is READY')

    local taken = tube:take(.1)
    test:ok(taken, 'task was taken')
    test:is(taken[3], 'abc', 'task.data')
    test:is(taken[2], state.TAKEN, 'task is really taken')

    local buried = tube:bury(task[1])
    test:ok(buried, 'tube:bury')
    test:is(buried[3], 'abc', 'buried.data')
    test:is(buried[2], state.BURIED, 'buried.status')

    local s, e = pcall(function() tube:ack(task[1]) end)
    test:ok(not s, "Can't ack removed task")
    test:ok(string.match(e, 'Task was not taken') ~= nil, 'peek error message')

    local peek = tube:peek(task[1])
    test:is(peek[1], buried[1], 'task was peek')
    test:is(peek[2], buried[2], 'task.status')
    test:is(peek[3], buried[3], 'task.data')

    fiber.create(function()
        local retaken = tube:take(0.1)
        test:ok(retaken, 'buried task was retaken')

        test:is(retaken[1], buried[1], 'task.id')
        test:is(retaken[2], state.TAKEN, 'task.status')
        test:is(retaken[3], buried[3], 'task.data')
    end)

    tube:kick(1)
    fiber.sleep(0.1)
end)

test:test('if_not_exists test', function(test)
    test:plan(2)
    local tube = queue.create_tube('test_ine', 'fifottl', {
        if_not_exists = true, engine = engine
    })
    local tube_new = queue.create_tube('test_ine', 'fifottl', {
        if_not_exists = true, engine = engine
    })
    test:is(tube, tube_new, "if_not_exists if tube exists")

    queue.tube['test_ine'] = nil
    local tube_new = queue.create_tube('test_ine', 'fifottl', {
        if_not_exists = true, engine = engine
    })
    test:isnt(tube, tube_new, "if_not_exists if tube doesn't exists")
end)

test:test('touch test', function(test)
    test:plan(3)
    tube:put('abc', {ttl=0.2, ttr=0.1})
    local task = tube:take()
    tube:touch(task[1], 0.3)
    fiber.sleep(0.1)
    test:is(task[2], 't')
    task = tube:ack(task[1])
    test:is(task[2], '-')
    test:isnt(task, nil)
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
    local tube = queue.create_tube('test_ttl_release', 'fifottl', { if_not_exists = true })
    tube:put({'test_task'}, { ttl = 10, ttr = 20 })
    tube:take()
    tube:release(0, { delay = DELTA })
    local task = box.space.test_ttl_release:get(0)
    test:is(task.ttl, (TTL + DELTA) * 1000000, 'check TTL after release')
    test:is(task.ttr, TTR * 1000000, 'check TTR after release')
end)

-- gh-96: infinite loop after dropping a tube with a burried task
test:test('buried task in a dropped queue', function(test)
    test:plan(1)

    local TASK_ID = 1
    local tube = queue.create_tube('test_drop_with_burried', 'fifottl',
        {ttr = 0.1, if_not_exist = true})

    tube:put({foo = 'bar'})
    local task = tube:take(0)
    tube:bury(task[TASK_ID])

    tube:drop()
    fiber.sleep(0.2)
    test:ok(true, 'queue does not hang')
end)

test:test('Space of queue is corrupted', function(test)
    test:plan(3)

    local fifottl = require('queue.abstract.driver.fifottl')
    local space = fifottl.create_space('corrupted_fifottl_space', { engine = engine })
    
    local task_id = space.index.task_id
    local status = space.index.status
    local watch = space.index.watch

    test:test('task_id index does not exist', function(test)
        test:plan(2)

        space.index.task_id = nil
        space.index.status = status
        space.index.watch = watch

        local q, e = pcall(fifottl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have task_id index') ~= nil, 'text of exception')
    end)

    test:test('status index does not exist', function(test)
        test:plan(2)

        space.index.task_id = task_id
        space.index.status = nil
        space.index.watch = watch

        local q, e = pcall(fifottl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have status index') ~= nil, 'text of exception')
    end)

    test:test('watch index does not exist', function(test)
        test:plan(2)

        space.index.task_id = task_id
        space.index.status = status
        space.index.watch = nil

        local q, e = pcall(fifottl.new, space)
        test:ok(not q, 'exception was thrown')
        test:ok(e:match('space does not have watch index') ~= nil, 'text of exception')
    end)
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
