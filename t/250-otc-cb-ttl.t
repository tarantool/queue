#!/usr/bin/env tarantool
local test = require('tap').test()
test:plan(2)

local fiber = require('fiber')

local tnt = require('t.tnt')
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'memtx'

local queue = require('queue')
local state = require('queue.abstract.state')

-- In the "ttl tasks" loop of the driver fiber the loop variable shadowed the
-- 'state' module, so `state.DONE` was indexing a string and evaluated to nil.
-- As a result the task reported to the on_task_change callback had its status
-- field set to NULL instead of state.DONE.
local function check_ttl_status(test, tube_name, driver, put_opts)
    local ttl_task

    local tube = queue.create_tube(tube_name, driver, {
        engine = engine,
        ttl    = 0.1,
        on_task_change = function(task, stats_data)
            if stats_data == 'ttl' then
                ttl_task = task
            end
        end
    })

    tube:put({'expired'}, put_opts)

    local deadline = fiber.clock() + 30
    while ttl_task == nil and fiber.clock() < deadline do
        fiber.sleep(0.05)
    end

    if ttl_task == nil then
        test:fail('on_task_change is called with the "ttl" event')
        test:fail('the expired task is reported in the DONE state')
        return
    end

    test:ok(true, 'on_task_change is called with the "ttl" event')
    test:is(ttl_task[2], state.DONE,
        'the expired task is reported in the DONE state')
end

test:test('fifottl', function(test)
    test:plan(2)
    check_ttl_status(test, 'otc_ttl_fifottl', 'fifottl', {})
end)

test:test('utubettl', function(test)
    test:plan(2)
    check_ttl_status(test, 'otc_ttl_utubettl', 'utubettl', {utube = 'u'})
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
