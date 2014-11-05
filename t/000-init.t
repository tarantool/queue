#!/usr/bin/env tarantool
-- vim: set ft=lua :


local queue = require 'queue'
local fiber = require 'fiber'
local test = (require 'tap').test()
test:plan(2)

test:test('access to queue until box.cfg is started', function(test)
    test:plan(3)
    test:isnil(rawget(box, 'space'), 'box is not started yet')

    local s, e = pcall(function() return queue.tube end)
    test:ok(not s, 'exception was generated')
    test:ok(string.match(e, 'Please run box.cfg') ~= nil, 'Exception text')
end)


local tnt  = require 't.tnt'
local state = require 'queue.abstract.state'
local yaml = require 'yaml'


test:test('access to queue after box.cfg{}', function(test)
    test:plan(9)
    test:istable(queue.tube, 'queue.tube is table')
    test:is(#queue.tube, 0, 'queue.tube is empty')

    local tube = queue.create_tube('test', 'fifo')
    test:ok(queue.tube.test, 'tube "test" is created')

    test:ok(queue.tube.test:put(123), 'put')

    local task = queue.tube.test:take(.1)
    test:ok(task, 'task was taken')
    test:is(task[3], 123, 'task.data')
    test:ok(queue.tube.test:ack(task[1]), 'task.ack')

    test:ok(queue.tube.test:drop(), 'tube.drop')
    test:isnil(queue.tube.test, 'tube is really removed')
end)



tnt.finish()
test:check()
os.exit(0)
