#!/usr/bin/env tarantool
local test = require('tap').test()
test:plan(1)

local tnt = require('t.tnt')
tnt.cfg{}

local queue = require('queue')

local function tube_check_simple(tube)
    tube:put{123}
    local task = tube:take(0)
    tube:ack(task[1])
end

test:test('on_task_change callback', function(test)
    test:plan(5)
    local cnt = 0

    -- simple callbacks, to check that they're being called
    local function cb(t1, t2) cnt = cnt + 1 end
    local function new_cb(t1, t2) cnt = cnt + 2 end

    -- init with initial callback
    local tube = queue.create_tube('test2', 'fifo', {
        on_task_change = cb
    })

    tube_check_simple(tube)
    test:is(cnt, 3, 'check counter')

    -- set new callback
    test:is(tube:on_task_change(new_cb), cb, "check rv of on_task_change")

    tube_check_simple(tube)
    test:is(cnt, 9, 'check counter after new cb is applied')

    -- delete callback
    test:is(tube:on_task_change(), new_cb, "check rv of on_task_change")

    tube_check_simple(tube)
    test:is(cnt, 9, 'check counter after new cb is applied')
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
