#!/usr/bin/env tarantool

local os = require('os')
local queue = require('queue')
local tap = require('tap')
local tnt = require('t.tnt')

local test = tap.test('custom driver registration after reload')
test:plan(2)

tnt.cfg()

--- Accept gh-137, we need to check custom driver registration
-- after restart. Instead of tarantool reboot, we will additionally
-- call queue.start() to simulate the reload of the module. This
-- is not a clean enough, because queue module doesn't provide the
-- hot restart.
--
-- All tricks in this test are done by professionals, don't try
-- to repeat it yourself!!!
local function check_driver_registration_after_reload()
    local fifo = require('queue.abstract.driver.fifo')
    queue.register_driver('fifo_cust', fifo)

    local tube = queue.create_tube('tube_cust', 'fifo_cust')
    tube:put('1')
    local task_id = tube:take()[1]

    -- Simulate the module reload.
    queue.driver.fifo_cust = nil
    queue.start()

    -- Check the task has been released after reload.
    queue.register_driver('fifo_cust', fifo)
    local task_status = queue.tube.tube_cust:peek(task_id)[2]
    test:is(task_status, 'r', 'check driver registration after reload')
end

check_driver_registration_after_reload()

local consumers = box.space._queue_consumers
consumers:format({
    {name = 'connection_id', type = 'unsigned'},
    {name = 'fiber_id', type = 'unsigned'},
    {name = 'tube_id', type = 'unsigned'},
    {name = 'event_time', type = 'unsigned'},
    {name = 'fiber_time', type = 'unsigned'}
})
queue.start()
test:ok(consumers:format()[6].name == 'consumer_group' and
    consumers.index.consumer.parts[2].fieldno == 6,
    'upgrade consumer space format')

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
