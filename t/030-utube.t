#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local test = (require('tap')).test()
test:plan(12)

local queue = require('queue')
local state = require('queue.abstract.state')

local tnt  = require('t.tnt')
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'memtx'

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'utube', { engine = engine })
local tube2 = queue.create_tube('test_stat', 'utube', { engine = engine })
local tube_ready, tube2_ready
tube_ready = queue.create_tube('test_ready', 'utube',
    { engine = engine, storage_mode = queue.driver.utube.STORAGE_MODE_READY_BUFFER })
tube2_ready = queue.create_tube('test_stat_ready', 'utube',
    { engine = engine, storage_mode = queue.driver.utube.STORAGE_MODE_READY_BUFFER })
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'utube', 'tube.type')

test:test('Utube statistics', function(test)
    test:plan(13 * 2)
    for _, tube_stat in ipairs({tube2, tube2_ready}) do
        if tube_stat == nil then
            break
        end

        tube_stat:put('stat_0')
        tube_stat:put('stat_1')
        tube_stat:put('stat_2')
        tube_stat:put('stat_3')
        tube_stat:put('stat_4')
        tube_stat:delete(4)
        tube_stat:take(.001)
        tube_stat:release(0)
        tube_stat:take(.001)
        tube_stat:ack(0)
        tube_stat:bury(1)
        tube_stat:bury(2)
        tube_stat:kick(1)
        tube_stat:take(.001)

        local stats = queue.statistics(tube_stat.name)

        -- check tasks statistics
        test:is(stats.tasks.taken, 1, 'tasks.taken')
        test:is(stats.tasks.buried, 1, 'tasks.buried')
        test:is(stats.tasks.ready, 1, 'tasks.ready')
        test:is(stats.tasks.done, 2, 'tasks.done')
        test:is(stats.tasks.delayed, 0, 'tasks.delayed')
        test:is(stats.tasks.total, 3, 'tasks.total')

        -- check function call statistics
        test:is(stats.calls.delete, 1, 'calls.delete')
        test:is(stats.calls.ack, 1, 'calls.ack')
        test:is(stats.calls.take, 3, 'calls.take')
        test:is(stats.calls.kick, 1, 'calls.kick')
        test:is(stats.calls.bury, 2, 'calls.bury')
        test:is(stats.calls.put, 5, 'calls.put')
        test:is(stats.calls.release, 1, 'calls.release')
    end
end)


test:test('Easy put/take/ack', function(test)
    test:plan(12 * 2)

    for _, test_tube in ipairs({tube, tube_ready}) do
        if test_tube == nil then
            break
        end

        test:ok(test_tube:put(123, {utube = 1}), 'task was put')
        test:ok(test_tube:put(345, {utube = 1}), 'task was put')
        local task = test_tube:take()
        test:ok(task, 'task was taken')
        test:is(task[2], state.TAKEN, 'task status')
        test:is(task[3], 123, 'task.data')
        test:ok(test_tube:take(.1) == nil, 'second task was not taken (the same tube)')

        task = test_tube:ack(task[1])
        test:ok(task, 'task was acked')
        test:is(task[2], '-', 'task status')
        test:is(task[3], 123, 'task.data')

        task = test_tube:take(.1)
        test:ok(task, 'task2 was taken')
        test:is(task[3], 345, 'task.data')
        test:is(task[2], state.TAKEN, 'task.status')
    end
end)

test:test('ack in utube', function(test)
    test:plan(8 * 2)

    for _, test_tube in ipairs({tube, tube_ready}) do
        if test_tube == nil then
            break
        end

        test:ok(test_tube:put(123, {utube = 'abc'}), 'task was put')
        test:ok(test_tube:put(345, {utube = 'abc'}), 'task was put')

        local state = 0
        fiber.create(function()
            fiber.sleep(0.1)
            local taken = test_tube:take()
            test:ok(taken, 'second task was taken')
            test:is(taken[3], 345, 'task.data')
            state = state + 1
        end)

        local taken = test_tube:take(.1)
        state = 1
        test:ok(taken, 'task was taken')
        test:is(taken[3], 123, 'task.data')
        fiber.sleep(0.3)
        test:is(state, 1, 'state was not changed')
        test_tube:ack(taken[1])
        fiber.sleep(0.2)
        test:is(state, 2, 'state was changed')
    end
end)
test:test('bury in utube', function(test)
    test:plan(8 * 2)

    for _, test_tube in ipairs({tube, tube_ready}) do
        if test_tube == nil then
            break
        end

        test:ok(test_tube:put(567, {utube = 'cde'}), 'task was put')
        test:ok(test_tube:put(789, {utube = 'cde'}), 'task was put')

        local state = 0
        fiber.create(function()
            fiber.sleep(0.1)
            local taken = test_tube:take()
            test:ok(taken, 'second task was taken')
            test:is(taken[3], 789, 'task.data')
            state = state + 1
        end)

        local taken = test_tube:take(.1)
        state = 1
        test:ok(taken, 'task was taken')
        test:is(taken[3], 567, 'task.data')
        fiber.sleep(0.3)
        test:is(state, 1, 'state was not changed')
        test_tube:bury(taken[1])
        fiber.sleep(0.2)
        test:is(state, 2, 'state was changed')
    end
end)
test:test('instant bury', function(test)
    test:plan(1 * 2)
    tube:put(1, {ttr=60})
    local taken = tube:take(.1)
    test:is(tube:bury(taken[1])[2], '!', 'task is buried')

    if tube_ready ~= nil then
        tube_ready:put(1, {ttr=60})
        local taken = tube_ready:take(.1)
        test:is(tube_ready:bury(taken[1])[2], '!', 'task is buried')
    end
end)

test:test('if_not_exists test', function(test)
    test:plan(2)
    local tube = queue.create_tube('test_ine', 'utube', {
        if_not_exists = true, engine = engine
    })
    local tube_new = queue.create_tube('test_ine', 'utube', {
        if_not_exists = true, engine = engine
    })
    test:is(tube, tube_new, "if_not_exists if tube exists")

    queue.tube['test_ine'] = nil
    local tube_new = queue.create_tube('test_ine', 'utube', {
        if_not_exists = true, engine = engine
    })
    test:isnt(tube, tube_new, "if_not_exists if tube doesn't exists")
end)

test:test('Get tasks by state test', function(test)
    test:plan(2)
    local tube = queue.create_tube('test_task_it', 'utube')

    for i = 1, 10 do
        tube:put('test_data' .. tostring(i), { utube = i })
    end
    for i = 1, 4 do
        tube:take(0.001)
    end

    local count_taken = 0
    local count_ready = 0

    for _, task in tube.raw:tasks_by_state(state.READY) do
        if task[2] == state.READY then
            count_ready = count_ready + 1
        end
    end

    for _, task in tube.raw:tasks_by_state(state.TAKEN) do
        if task[2] == state.TAKEN then
            count_taken = count_taken + 1
        end
    end

    test:is(count_ready, 6, 'Check tasks count in a ready state')
    test:is(count_taken, 4, 'Check tasks count in a taken state')
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
