#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local test = require('tap').test()
test:plan(8)

local queue = require('queue')
local state = require('queue.abstract.state')

local engine = os.getenv('ENGINE') or 'memtx'

local tnt = require('t.tnt')
tnt.cfg{}

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('lim3_tube', 'limfifottl', { engine = engine, capacity = 3 })
local unlim_tube = queue.create_tube('unlim_tube', 'limfifottl', { engine = engine })

test:ok(tube, 'test tube created')
test:is(tube.name, 'lim3_tube', 'tube.name')
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
        test:ok(tube:put({4}, {timeout = 1}), 'task 4 was put')
    end)

    local task = tube:take()
    test:ok(task, 'task 3 was taken')
    test:is(tube:ack(task[1])[2], state.DONE, 'task 3 is done')

    while put_fiber:status() ~= 'dead' do
        fiber.yield()
    end
end)

test:test('Unlimited tube put', function(test)
    test:plan(3)

    test:is(unlim_tube:take(0), nil, 'tube is empty')
    test:ok(unlim_tube:put{1}, 'task 1 was put')
    test:ok(unlim_tube:put{2}, 'task 2 was put')
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
