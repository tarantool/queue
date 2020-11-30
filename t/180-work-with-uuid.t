#!/usr/bin/env tarantool

local fiber = require('fiber')
local netbox = require('net.box')
local tap = require('tap')
local os = require('os')
local tnt  = require('t.tnt')

local test = tap.test('test work with uuid')
test:plan(4)

local listen = 'localhost:1918'
tnt.cfg{ listen = listen }

rawset(_G, 'queue', require('queue'))

test:test('test UUID format validation', function(test)
    test:plan(1)

    -- We need to call the `grant()` to have sufficient rights
    -- to call `identify.`
    local tube = queue.create_tube('test_tube', 'fifo')
    tube:grant('guest', { call = true })

    local conn = netbox.connect(listen)
    local invalid_uuid = 'just-for-fun'
    local ok, err = pcall(conn.call, conn, 'queue.identify', {invalid_uuid})

    local check_validation = not ok and err:match('Invalid UUID format.')
    test:ok(check_validation, 'UUID format validation has been checked.')
    conn:close()

    tube:drop()
end)

test:test('test work with two consumers with the same uuid', function(test)
    test:plan(4)

    -- Preparing of the tube.
    local tube = queue.create_tube('test_tube', 'fifo')
    tube:grant('guest', { call = true })
    local uuid_con1
    local uuid_con2
    tube:put('test_data')

    local cond = fiber.cond()

    local fiber_consumer_1 = fiber.new(function()
        local conn = netbox.connect(listen)
        local task = conn:call('queue.tube.test_tube:take')
        local task_id = task[1]
        uuid_con1 = conn:call('queue.identify')

        -- Wait until consumer 2 connects to the session.
        cond:signal()
        cond:wait()

        -- Reconnect to the session.
        conn:close()
        conn = netbox.connect(listen)
        uuid_con1 = conn:call('queue.identify', {uuid_con2})
        test:ok(uuid_con1 == uuid_con2,
            'reconnection of consumer 1 has been completed')

        -- Ack the task and close the connection.
        task = conn:call('queue.tube.test_tube:ack', {task_id})
        test:ok(task[1] == task_id and task[2] == '-', 'task has been acked')
        conn:close()

        -- Wakeup the consumer 2 fiber and wait for it to finishes.
        cond:signal()
        cond:wait()
    end)

    fiber_consumer_1:set_joinable(true)

    local fiber_consumer_2 = fiber.create(function()
        -- Waiting for consumer 1 identification
        cond:wait()

        -- Connect to server and connect to the same session as consumer 1.
        local conn = netbox.connect(listen)
        uuid_con2 = conn:call('queue.identify', {uuid_con1})
        test:ok(uuid_con1 == uuid_con2,
            'consumer 2 identification completed correctly')

        -- Wait until consumer 1 will reconnect and "ack" the task.
        cond:signal()
        cond:wait()

        -- Close the connection and wakeup the consumer 1 fiber.
        conn:close()
        cond:signal()
    end)

    -- Wait for consumers fibers to finishes.
    local ok = fiber_consumer_1:join()
    test:ok(ok, 'reconnection test done')

    tube:drop()
end)

test:test('test reconnect and ack the task', function(test)
    test:plan(2)

    -- Preparing of the tube.
    queue.cfg({ ttr = 60 })
    local tube = queue.create_tube('test_tube', 'fifo')
    tube:put('test_data')
    tube:grant('guest', { call = true })

    local fiber_consumer = fiber.new(function()
        -- Connect and take a task.
        local conn = netbox.connect(listen)
        local task = conn:call('queue.tube.test_tube:take')
        local task_id = task[1]
        local session_id = conn:call('queue.identify')
        conn:close()

        -- Reconnect and ack the task.
        conn = netbox.connect(listen)
        conn:call('queue.identify', {session_id})
        task = conn:call('queue.tube.test_tube:ack', {task_id})
        test:ok(task[1] == task_id and task[2] == '-', 'task has been acked')

        conn:close()
    end)

    fiber_consumer:set_joinable(true)

    local ok = fiber_consumer:join()
    test:ok(ok, 'reconnect and ack the task test done')

    tube:drop()
end)

test:test('test expiration', function(test)
    test:plan(5)

    -- Preparing of the tube.
    local tube = queue.create_tube('test_tube', 'fifo')
    tube:put('test_data')
    tube:grant('guest', { call = true })

    local fiber_consumer = fiber.new(function()
        queue.cfg({ ttr = 1 })
        -- Connect and take a task.
        local conn = netbox.connect(listen)
        local task_id = conn:call('queue.tube.test_tube:take')[1]
        local uuid_con = conn:call('queue.identify')
        conn:close()

        -- Check that the task is in a "taken" state before ttr expires.
        fiber.sleep(0.1)
        test:ok(tube:peek(task_id)[2] == 't', 'task in taken state')
        fiber.sleep(2)

        -- The task must be released after the ttr expires.
        test:ok(tube:peek(task_id)[2] == 'r', 'task in ready state after ttr')

        -- The old queue session must expire.
        conn = netbox.connect(listen)
        local ok, err = pcall(conn.call, conn, 'queue.identify',
            {uuid_con})
        local check_ident = not ok and err:match('UUID .* is unknown.')
        test:ok(check_ident, 'the old queue session has expired.')
        conn:close()

        -- If ttr = 0, the task should be released immediately.
        queue.cfg({ ttr = 0 })
        conn = netbox.connect(listen)
        task_id = conn:call('queue.tube.test_tube:take')[1]
        conn:close()
        fiber.sleep(0.1)
        test:ok(tube:peek(task_id)[2] == 'r',
            'task has been immediately released')
    end)

    fiber_consumer:set_joinable(true)

    local ok = fiber_consumer:join()
    test:ok(ok, 'expiration test done')

    tube:drop()
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
