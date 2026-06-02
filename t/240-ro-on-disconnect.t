#!/usr/bin/env tarantool

local log   = require('log')
local tnt   = require('t.tnt')
local test  = require('tap').test('')
local fiber = require('fiber')
local queue = require('queue')

local qc = require('queue.compat')
if not qc.check_version({2, 4, 1}) then
    log.info('Tests skipped, tarantool version < 2.4.1')
    return
end

rawset(_G, 'queue', require('queue'))

local session = require('queue.abstract.queue_session')
local queue_state = require('queue.abstract.queue_state')

test:plan(4)

test:test('on_disconnect handler must be RO-safe', function(test)
    test:plan(6)

    tnt.cluster.cfg{}
    test:ok(tnt.cluster.wait_replica(), 'wait for replica to connect')

    queue.cfg{ttr = 0.5, in_replicaset = true}
    local tube = queue.create_tube('test_ro_disc', 'fifo', {if_not_exists = true})
    test:ok(tube, 'tube created')

    local f = fiber.new(function()
        queue.tube.test_ro_disc:take(3600)
    end)
    f:name('queue_waiter_fiber')

    local ok = false
    for _ = 1, 300 do
        if box.space._queue_consumers:count() > 0 then
            ok = true
            break
        end
        fiber.sleep(0.01)
    end
    test:ok(ok, 'waiter registered in _queue_consumers')

    box.cfg{read_only = true}
    test:ok(box.info.ro, 'instance is RO')

    local ok_call, err = pcall(queue._on_consumer_disconnect)
    test:ok(ok_call, ('_on_consumer_disconnect() must not fail on RO, err = %s'):format(tostring(err)))

    box.cfg{read_only = false}
    test:ok(not box.info.ro, 'instance back to RW')
end)

test:test('release_session_tasks: RO-safe', function(test)
    test:plan(10)

    local tube = queue.create_tube('test_rel', 'fifo', {if_not_exists = true})
    test:ok(tube, 'tube created')

    -- Create a session + take a task so _queue_taken_2 has a record.
    local client = tnt.cluster.connect_master()
    test:ok(client.error == nil, 'client connected')

    local session_uuid = client:call('queue.identify')
    test:ok(session_uuid ~= nil, 'got session_uuid')

    test:ok(queue.tube.test_rel:put('data'), 'put task')
    local task = client:call('queue.tube.test_rel:take')
    test:ok(task ~= nil, 'task taken')

    local taken_before = box.space._queue_taken_2.index.uuid:select{session_uuid}
    test:is(#taken_before, 1, '_queue_taken_2 has 1 record before')

    box.cfg{read_only = true}
    test:ok(queue_state.poll(queue_state.states.WAITING, 10), 'state WAITING')
    test:ok(box.info.ro, 'instance is RO')

    local ok_call, err = pcall(session._on_session_remove, session_uuid)
    test:ok(ok_call, ('on_session_remove does not fail on RO, err=%s'):format(tostring(err)))

    local taken_after = box.space._queue_taken_2.index.uuid:select{session_uuid}
    test:is(#taken_after, 1, '_queue_taken_2 unchanged on RO')

    -- Cleanup: back to RW.
    box.cfg{read_only = false}
    queue_state.poll(queue_state.states.RUNNING, 10)
    client:close()
end)

test:test('release_session_tasks: works on RW', function(test)
    test:plan(11)

    box.cfg{read_only = false}
    queue_state.poll(queue_state.states.RUNNING, 10)
    test:ok(not box.info.ro, 'instance is RW')

    queue.cfg{ttr = 0.5, in_replicaset = true}
    local tube = queue.create_tube('test_rel2', 'fifo', {if_not_exists = true})
    test:ok(tube, 'tube created')

    local client = tnt.cluster.connect_master()
    test:ok(client.error == nil, 'client connected')

    local session_uuid = client:call('queue.identify')
    test:ok(session_uuid ~= nil, 'got session_uuid')

    test:ok(queue.tube.test_rel2:put('data2'), 'put task')
    local task = client:call('queue.tube.test_rel2:take')
    test:ok(task ~= nil, 'task taken')

    local taken_before = box.space._queue_taken_2.index.uuid:select{session_uuid}
    test:is(#taken_before, 1, '_queue_taken_2 has 1 record before')

    -- Call on_session_remove callback directly on RW: must release task.
    local ok_call, err = pcall(session._on_session_remove, session_uuid)
    test:ok(ok_call, ('on_session_remove ok on RW, err=%s'):format(tostring(err)))

    -- Taken record must be removed.
    local taken_after = box.space._queue_taken_2.index.uuid:select{session_uuid}
    test:is(#taken_after, 0, '_queue_taken_2 record removed')

    -- Task must become READY again and be takeable.
    local task2 = client:call('queue.tube.test_rel2:take', {0})
    test:ok(task2 ~= nil, 'task is takeable again after release')
    test:is(task2[3], 'data2', 'task data preserved')

    client:close()
end)

test:test('cleanup_temp_spaces after RW->RO->RW', function(test)
    test:plan(12)

    box.cfg{read_only = false}
    queue_state.poll(queue_state.states.RUNNING, 10)
    test:ok(not box.info.ro, 'instance is RW')

    queue.cfg{ttr = 0.5, in_replicaset = true}
    local tube = queue.create_tube('test_cleanup', 'fifo', {if_not_exists = true})
    test:ok(tube ~= nil, 'tube created')

    local consumers = box.space._queue_consumers
    local sessions = box.space._queue_session_ids
    test:ok(consumers ~= nil, '_queue_consumers space exists')
    test:ok(sessions ~= nil, '_queue_session_ids space exists')

    local session_uuid = queue.identify()
    test:ok(session_uuid ~= nil, 'got session_uuid')
    test:isnt(sessions:count(), 0, '_queue_session_ids not empty after identify')

    -- Long waiting fiber.
    local f = fiber.new(function()
        queue.tube.test_cleanup:take(3600)
    end)
    f:name('cleanup_waiter')

    -- Wait for the waiter to be registered.
    local ok = false
    for _ = 1, 300 do
        if consumers:count() > 0 then
            ok = true
            break
        end
        fiber.sleep(0.01)
    end

    -- Waiter is registered and temporary spaces are not empty.
    test:ok(ok, 'waiter registered in _queue_consumers')
    test:isnt(consumers:count(), 0, '_queue_consumers not empty before RO')
    test:isnt(sessions:count(), 0, '_queue_session_ids not empty before RO')

    -- Imitate failover.
    box.cfg{read_only = true}
    fiber.sleep(0.1)

    box.cfg{read_only = false}
    queue_state.poll(queue_state.states.RUNNING, 10)
    test:ok(not box.info.ro, 'back to RW')

    -- Temporary spaces must be empty after the transition
    test:is(consumers:count(), 0, '_queue_consumers truncated')
    test:is(sessions:count(), 0, '_queue_session_ids truncated')

    f:cancel()
end)

rawset(_G, 'queue', nil)
tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
