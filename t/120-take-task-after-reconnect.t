#!/usr/bin/env tarantool

local fiber = require('fiber')
local netbox = require('net.box')
local os = require('os')
local queue = require('queue')
local tap = require('tap')
local tnt = require('t.tnt')


local test = tap.test('take a task after reconnect')
test:plan(1)

local listen = 'localhost:1918'
tnt.cfg{listen = listen}


local function test_take_task_after_disconnect(test)
    test:plan(1)
    local driver = 'fifottl'
    local tube = queue.create_tube('test_tube', driver,
        {if_not_exists = true})
    rawset(_G, 'queue', require('queue'))
    tube:grant('guest', {call = true})
    local task_id = tube:put('test_data')[1]
    -- Now we have one task in a ready state

    local connection = netbox.connect(listen)
    local fiber_1 = fiber.create(function()
        connection:call('queue.tube.test_tube:take')
        connection:call('queue.tube.test_tube:take')
    end)

    -- This is not a best practice but we need to use the fiber.sleep()
    -- (not fiber.yield()).
    -- Expected results from a sleep() calling:
    -- 1) Execute first connection:call('queue.tube.test_tube:take')
    --    Now one task in a taken state
    -- 2) Call the second connection:call('queue.tube.test_tube:take')
    --  and to hang the fiber_1
    -- 3) Start a fiber on the server side of connection which will execute
    --  second queue.tube.test_tube:take call and hang because the queue
    --  is empty
    fiber.sleep(0.1)

    connection:close()

    fiber.sleep(0.1)
    -- The taken task will be released (cause - disconnection).
    -- After that the fiber which waiting of a ready task (into take procedure)
    -- will try to take this task (before the fix).


    test:is(tube:peek(task_id)[2] == 'r', true, 'Task in ready state')
end


test:test('Don\'t take a task after disconnect', test_take_task_after_disconnect)


tnt.finish()
os.exit(test:check() and 0 or 1)
