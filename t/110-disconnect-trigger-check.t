#!/usr/bin/env tarantool

local fiber = require('fiber')
local netbox = require('net.box')
local os = require('os')
local queue = require('queue')
local tap = require('tap')
local tnt = require('t.tnt')

local tube
local test = tap.test('lost a session id after yield')

-- The test cases are in check_result().
test:plan(2)

-- Verify that _queue_taken space is empty.
local function check_result()
    if tube == nil then
        return
    end

    -- tube:drop() is most simple way to check that _queue_taken
    -- is empty. It give an error if it is not so.
    local ok, res = pcall(tube.drop, tube)
    test:is(ok, true, 'drop empty queue')
    test:is(res, true, 'tube:drop() result is true')

    tnt.finish()
end

-- Yield in queue's on_disconnect trigger (which handles a client
-- disconnection) may lead to a situation when _queue_taken
-- temporary space is not cleaned and becomes inconsistent with
-- 'status' field in <tube_name> space. This appears only on
-- tarantool versions affected by gh-4627.
--
-- See https://github.com/tarantool/queue/issues/103
-- See https://github.com/tarantool/tarantool/issues/4627
local function test_lost_session_id_after_yield()
    -- We must check the results of a test after
    -- the queue._on_consumer_disconnect trigger
    -- has been done.
    --
    -- Triggers are run in LIFO order.
    box.session.on_disconnect(check_result)

    local listen = 'localhost:1918'
    tnt.cfg{listen = listen}

    local driver = 'fifottl'
    tube = queue.create_tube('test_tube', driver, {if_not_exists = true})

    rawset(_G, 'queue', require('queue'))
    tube:grant('guest', {call = true})

    -- We need at least two tasks to trigger box.session.id()
    -- call after a yield in the queue._on_consumer_disconnect
    -- trigger (in the version of queue before the fix). See
    -- more below.
    queue.tube.test_tube:put('1')
    queue.tube.test_tube:put('2')
    local connection = netbox.connect(listen)
    connection:call('queue.tube.test_tube:take')
    connection:call('queue.tube.test_tube:take')

    -- After disconnection of a client the _on_consumer_disconnect
    -- trigger is run. It changes 'status' field for tuples in
    -- <tube_name> space in a loop and removes the corresponding
    -- tuples from _queue_taken space. The version before the fix
    -- operates in this way:
    --
    --  | <_on_consumer_disconnect>
    --  | for task in tasks of the client:
    --  |     call <task:release>
    --  |
    --  | <task:release>
    --  |     delete _queue_taken tuple using box.session.id()
    --  |     update <tube_name> space using task_id -- !! yield
    --
    -- So the deletion from _queue_taken may be unable to delete
    -- right tuples for second and following tasks, because
    -- box.session.id() may give a garbage.
    connection:close()

    -- Wait for check_result() trigger, which will ensure that
    -- _queue_taken space is cleaned and will exit successfully
    -- in the case (or exit abnormally otherwise).
    fiber.sleep(5)

    -- Wrong session id may lead to 'Task was not taken in the
    -- session' error in the _on_consumer_disconnect and so the
    -- second on_disconnect trigger (check_result) will not be
    -- fired.
    os.exit(test:check() and 0 or 1)
end

test_lost_session_id_after_yield()
-- vim: set ft=lua :
