#!/usr/bin/env tarantool

-- vim: set ft=lua :

local fiber = require 'fiber'
local test = (require 'tap').test()
local tnt  = require 't.tnt'
local state = require 'queue.abstract.state'
test:plan(7)

local queue = require 'queue.abstract'
test:ok(queue.start(), 'queue.start()')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'fifottl')
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'fifottl', 'tube.type')


test:test('put/take/peek', function(test)
    test:plan(11)

    local task = tube:put('abc')

    test:ok(task, "task was put")
    test:is(task[2], state.READY, "task.state")
    
    local peek = tube:peek(task[1])
    test:isdeeply(task[1], peek[1], "put and peek tasks are the same")
    test:isdeeply(task[2], peek[2], "put and peek tasks are the same")
    test:isdeeply(task[3], peek[3], "put and peek tasks are the same")

    local taken = tube:take{ timeout =  .1 }
    test:ok(taken, 'task was taken')

    test:is(task[1], taken[1], 'task.id')
    test:is(taken[2], state.TAKEN, 'task.status')
    
    local ack = tube:ack(taken[1])
    test:ok(ack, 'task was acked')

    local s, e = pcall(function() tube:peek(task[1]) end)
    test:ok(not s, "peek status")
    test:is(e, 'Task not found', 'peek error message')
end)

test:test('delayed tasks', function(test)
    test:plan(4)

    local task = tube:put('cde', { delay = 0.1 })
    test:ok(task, 'delayed task was put')
    test:is(task[2], state.DELAYED, 'state is DELAYED')

    test:isnil(tube:take(.01), 'delayed task was not taken')

    local taken = tube:take(.15)
    test:ok(taken, 'delayed task was taken after timeout')
end)


test:check()
tnt.finish()
os.exit(0)

