#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local test = require('tap').test()
test:plan(9)

local queue = require('queue')
local state = require('queue.abstract.state')

local engine = os.getenv('ENGINE') or 'memtx'

local tnt = require('t.tnt')
tnt.cfg{}

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'limfifottl', { engine = engine, capacity = 3 })
local tube2 = queue.create_tube('test_stat', 'limfifottl', { engine = engine })

test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'limfifottl', 'tube.type')

test:test('Put timeout is reached', function(test)
    test:plan(4)

    test:ok(tube:put{1}, 'task 1 was put')
    test:ok(tube:put{2}, 'task 2 was put')
    test:ok(tube:put{3}, 'task 3 was put')
    test:is(tube:put({4}, {timeout = 0.1}), nil, 'task 4 wasn\'t put cause timeout')
end)

test:test('Put after freeing up space', function(test)
    test:plan(3)
    local put_fiber = fiber.create(function()
        test:ok(tube:put({4}, {timeout = 10}), 'task 4 was put')
    end)

    local task = tube:take()
    test:ok(task, 'task 3 was taken')
    test:is(tube:ack(task[1])[2], state.DONE, 'task 3 is done')

    while put_fiber:status() ~= 'dead' do
        fiber.yield()
    end
end)

test:test('Put with delay, timeout is not reached', function(test)
    test:plan(3)

    local put_fiber = fiber.create(function()
        test:ok(tube:put({4}, {timeout = 0.01, delay = 1}), 'task 4 was put and timeout was not reached')
    end)

    local task = tube:take()
    test:ok(task, 'task 4 was taken')
    test:is(tube:ack(task[1])[2], state.DONE, 'task 4 is done')

    fiber.sleep(0.1)
end)

test:test('Limfifottl statistics', function(test)
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

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
