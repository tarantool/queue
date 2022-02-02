#!/usr/bin/env tarantool

local os = require('os')

local fiber = require('fiber')

local queue = require('queue')
local state = require('queue.abstract.state')

local tap = require('tap')
local tnt = require('t.tnt')

local test = tap.test('work with "ttl" of buried task.')
test:plan(1)

-- Fields in the task tuple.
local TASK_ID = 1
local TASK_STATE = 2

tnt.cfg{}

test:test('test work with "ttl", when "bury" after "take"', function(test)
    -- Before the patch if a task has been "buried" after it was "taken"
    -- (and the task has "ttr") when the time in `i_next_event` will be
    -- interpreted as "ttl" in `{fifottl,utubettl}_fiber_iteration` and
    -- the task will be deleted.
    local drivers = {'fifottl', 'utubettl'}
    test:plan(3 * table.getn(drivers))

    local TTR = 0.2
    local TTL = 1

    for _, driver in pairs(drivers) do
        local tube = queue.create_tube('test_tube', driver, {if_not_exists = true})
        local task = tube:put('task1', {ttl = TTL, ttr = TTR})

        -- "Take" a task and "bury" it.
        task = tube:take(0)
        local id = task[TASK_ID]
        tube:bury(id)

        -- Check status of the task.
        task = tube:peek(id)
        test:is(task[TASK_STATE], state.BURIED,
            ('task "buried", driver: "%s"'):format(driver))

        -- Check status of the task after "ttr" has expired.
        fiber.sleep(TTR * 2)
        task = tube:peek(id)
        test:is(task[TASK_STATE], state.BURIED,
            ('task is still "buried", driver: "%s"'):format(driver))

        -- Check status of the task after "ttl" has expired.
        fiber.sleep(TTL * 2)
        ok, res = pcall(tube.peek, tube, id)
        test:ok(res:match(string.format('Task %d not found', id)),
            ('task done, driver: "%s"'):format(driver))

        tube:drop()
    end
end)

tnt.finish()
os.exit(test:check() and 0 or 1)

-- vim: set ft=lua :
