#!/usr/bin/env tarantool
-- vim: set ft=lua :

local fiber = require 'fiber'
local test = (require 'tap').test()
local tnt  = require 't.tnt'
local state = require 'queue.abstract.state'
local yaml = require 'yaml'
test:plan(9)

test:ok(rawget(box, 'space'), 'box started')

local queue = require 'queue.abstract'
test:ok(queue.start(), 'queue.start()')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'stube')
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'stube', 'tube.type')

test:test('Easy put/take/ack', function(test)
    test:plan(12)

    test:ok(tube:put(123, {stube = 1}), 'task was put')
    test:ok(tube:put(345, {stube = 1}), 'task was put')
    local task = tube:take(nil, {stube = 1})
    test:ok(task, 'task was taken')
    test:is(task[2], state.TAKEN, 'task status')
    test:is(task[3], 123, 'task.data')
    test:ok(tube:take(.1, {stube = 2}) == nil, 'second task was not taken (other tube)')

    task = tube:ack(task[1])
    test:ok(task, 'task was acked')
    test:is(task[2], '-', 'task status')
    test:is(task[3], 123, 'task.data')

    task = tube:take(.1, {stube = 1})
    test:ok(task, 'task2 was taken')
    test:is(task[3], 345, 'task.data')
    test:is(task[2], state.TAKEN, 'task.status')
end)

test:test('ack in stube', function(test)
    test:plan(6)

    test:ok(tube:put(123, {stube = 'abc'}), 'task was put')
    test:ok(tube:put(345, {stube = 'abc'}), 'task was put')

    local taken = tube:take(.1, {stube = 'abc'})
    test:ok(taken, 'task was taken')
    test:is(taken[3], 123, 'task.data')

    local taken = tube:take(nil, {stube = 'abc'})
    test:ok(taken, 'second task was taken') 
    test:is(taken[3], 345, 'task.data')
    tube:ack(taken[1])
end)

test:test('bury in stube', function(test)
    test:plan(10)

    test:ok(tube:put(567, {stube = 'cde'}), 'task was put')
    test:ok(tube:put(789, {stube = 'cde'}), 'task was put')

    local taken = tube:take(.1, {stube = 'cde'})
    test:ok(taken, 'task was taken')
    test:is(taken[3], 567, 'task.data')
    test:is(tube:bury(taken[1])[2], state.BURIED, 'task was bury')
    test:is(tube:kick(1), 1, 'task was kick')

    local taken = tube:take(nil, {stube = 'cde'})
    test:ok(taken, 'second task was taken after bury') 
    test:is(taken[3], 567, 'task.data')

    local taken = tube:take(nil, {stube = 'cde'})
    test:ok(taken, 'second task was taken') 
    test:is(taken[3], 789, 'task.data')
end)


tnt.finish()
test:check()
os.exit()
