#!/usr/bin/env tarantool
-- vim: set ft=lua :

local fiber = require 'fiber'
local test = (require 'tap').test()
local tnt  = require 't.tnt'
local state = require 'queue.abstract.state'
local yaml = require 'yaml'
test:plan(10)

test:ok(rawget(box, 'space'), 'box started')

local queue = require 'queue.abstract'
test:ok(queue.start(), 'queue.start()')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'utube')
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'utube', 'tube.type')

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
    taken = tube:take(.1)
    test:is(tube:bury(taken[1])[2], '!', 'task is buried')
end)


tnt.finish()
test:check()
os.exit()
