#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local test  = require('tap').test()
test:plan(6)

local queue = require('queue')
local state = require('queue.abstract.state')

local tnt   = require('t.tnt')
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'memtx'

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'fifo', { engine = engine })
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'fifo', 'tube.type')

test:test('concurent take', function(test)
    test:plan(16)

    local channel = fiber.channel(1000)
    test:ok(channel, 'channel created')

    local res = {}
    for i = 1, 5 do
        fiber.create(function(i)
            local taken = tube:take(1)
            test:ok(taken, 'Task was taken ' .. i)
            table.insert(res, { taken })
            channel:put(true)
        end, i)
    end

    fiber.sleep(.1)
    test:ok(tube:put(1), 'task 1 was put')

    for i = 2, 5 do
        fiber.create(function(i)
            fiber.sleep(.5 / i)
            test:ok(tube:put(i), 'task ' .. i .. ' was put')
        end, i)
    end

    for i = 1, 5 do
        test:ok(channel:get(1 / i), 'take was done ' .. i)
    end
end)


tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua:
