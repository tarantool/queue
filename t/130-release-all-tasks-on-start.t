#!/usr/bin/env tarantool

local os = require('os')
local queue = require('queue')
local tap = require('tap')
local tnt = require('t.tnt')

local test = tap.test('release all tasks on start')
test:plan(1)

---
-- Accept gh-66, we need to release all taken tasks on start.
-- Instead of tarantool reboot, we will additionally call queue.start()
-- to simulate the reload of the module. This is not a clean enough,
-- because fibers launched by the module are not cleaned up.
-- See https://github.com/tarantool/queue/issues/66
--
-- All tricks in this test are done by professionals, don't try
-- to repeat it yourself!!!
local function check_release_tasks_on_start()
    tnt.cfg()
    -- The "fifottl" driver was choosen for check gh-121.
    -- We cann't use opts == nil as argument for driver "release"
    -- method. This is the policy of the module "queue" (check
    -- opts in abstract.lua, instead to check inside the driver).
    -- See https://github.com/tarantool/queue/issues/121
    local driver = 'fifottl'
    local tube = queue.create_tube('test_tube', driver)

    tube:put('1')
    tube:put('2')
    tube:put('3')

    tube:take()
    tube:take()
    tube:take()

    -- Simulate the module reload.
    queue.start()

    local ready_tasks_num = queue.statistics()['test_tube']['tasks']['ready']
    test:is(ready_tasks_num, 3, 'check release tasks on start')
end

check_release_tasks_on_start()

tnt.finish()
os.exit(test:check() and 0 or 1)
