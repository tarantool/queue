#!/usr/bin/env tarantool

local os = require('os')
local fiber = require('fiber')
local log = require('log')
local tnt = require('t.tnt')
local test = require('tap').test('subqueuettl')

local queue = require('queue')
local state = require('queue.abstract.state')
local queue_state = require('queue.abstract.queue_state')
local qc = require('queue.compat')

test:plan(21)
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'memtx'
if engine == 'vinyl' then
    print('1..0 # SKIP subqueuettl does not support vinyl engine')
    os.exit(0)
end
local tube = queue.create_tube('subqueue', 'subqueuettl', {engine = engine})
local tube_stat = queue.create_tube('subqueue_stat', 'subqueuettl', {engine = engine})

-- Counts the number of subqueues reported in stats.driver.subqueues.
local function count_subqueues(stats)
    local n = 0
    for _ in pairs(stats.extra.subqueues) do
        n = n + 1
    end
    return n
end

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')
test:ok(tube, 'test tube created')
test:is(tube.name, 'subqueue', 'tube.name')
test:is(tube.type, 'subqueuettl', 'driver is registered')

test:test('statistics', function(test)
    test:plan(23)
    for i = 0, 4 do
        tube_stat:put('stat_' .. i, {subqueue = 'stat_' .. i})
    end

    tube_stat:put('stat_5', {subqueue = 'stat_5', delay = 1000})
    tube_stat:delete(4)
    tube_stat:take(.001, {subqueue = 'stat_0'})
    tube_stat:release(0)
    tube_stat:take(.001, {subqueue = 'stat_0'})
    tube_stat:ack(0)
    tube_stat:bury(1)
    tube_stat:bury(2)
    tube_stat:kick(1)
    tube_stat:take(.001, {subqueue = 'stat_1'})

    local stats = queue.statistics(tube_stat.name)

    test:is(stats.tasks.taken, 1, 'tasks.taken')
    test:is(stats.tasks.buried, 1, 'tasks.buried')
    test:is(stats.tasks.ready, 1, 'tasks.ready')
    test:is(stats.tasks.done, 2, 'tasks.done')
    test:is(stats.tasks.delayed, 1, 'tasks.delayed')
    test:is(stats.tasks.total, 4, 'tasks.total')

    test:is(stats.calls.delete, 1, 'calls.delete')
    test:is(stats.calls.ack, 1, 'calls.ack')
    test:is(stats.calls.take, 3, 'calls.take')
    test:is(stats.calls.kick, 1, 'calls.kick')
    test:is(stats.calls.bury, 2, 'calls.bury')
    test:is(stats.calls.put, 6, 'calls.put')
    test:is(stats.calls.release, 1, 'calls.release')

    test:is(stats.extra.subqueues.stat_1.taken, 1, 'subqueue taken task count')
    test:is(stats.extra.subqueues.stat_2.buried, 1, 'subqueue buried task count')
    test:is(stats.extra.subqueues.stat_3.ready, 1, 'subqueue ready task count')
    test:is(stats.extra.subqueues.stat_5.delayed, 1, 'subqueue delayed task count')
    test:is(stats.extra.subqueues.stat_5.total, 1, 'subqueue total task count')

    -- All subqueue names are recorded in the best-effort registry, even
    -- those whose tasks were later removed.
    test:is(count_subqueues(stats), 6, 'all six subqueue names recorded')

    -- Stale subqueues (all tasks removed via ack/delete) remain in the
    -- best-effort registry and are reported with zero counts.
    test:is(stats.extra.subqueues.stat_0.total, 0,
        'emptied stat_0 reports zero total')
    test:is(stats.extra.subqueues.stat_0.ready, 0,
        'emptied stat_0 reports zero ready')
    test:is(stats.extra.subqueues.stat_4.total, 0,
        'deleted stat_4 reports zero total')
    test:is(stats.extra.subqueues.stat_4.ready, 0,
        'deleted stat_4 reports zero ready')
end)

test:test('basic put, take, and ack', function(test)
    test:plan(11)
    test:ok(tube:put(123, {subqueue = 'basic'}), 'task was put')
    test:ok(tube:put(345, {subqueue = 'basic'}), 'task was put')

    local task = tube:take(.1, {subqueue = 'basic'})
    test:ok(task, 'task was taken')
    test:is(task[2], state.TAKEN, 'task status')
    test:is(task[3], 123, 'task.data')

    task = tube:ack(task[1])
    test:ok(task, 'task was acked')
    test:is(task[2], '-', 'task status')
    test:is(task[3], 123, 'task.data')

    task = tube:take(.1, {subqueue = 'basic'})
    test:ok(task, 'second task was taken')
    test:is(task[3], 345, 'task.data')
    test:is(task[2], state.TAKEN, 'task status')
    tube:ack(task[1])
end)

test:test('TTR', function(test)
    test:plan(3)
    local subqueue = 'ttr'

    test:ok(tube:put('ttr', {subqueue = subqueue, ttr = 1}), 'put TTR task')
    test:ok(tube:take(.1, {subqueue = subqueue}), 'take TTR task')
    fiber.sleep(1.1)

    local task = tube:peek(tube.raw.space.index.task_id:max()[1])
    test:is(task[2], state.READY, 'task becomes ready after TTR')

    task = tube:take(.1, {subqueue = subqueue})
    tube:ack(task[1])
end)

test:test('parallel take from one subqueue', function(test)
    test:plan(7)
    local subqueue = 'parallel'
    test:ok(tube:put(678, {subqueue = subqueue}), 'first task was put')
    test:ok(tube:put(890, {subqueue = subqueue}), 'second task was put')

    local session_uuid = queue.identify()
    local result = fiber.channel(2)

    for i = 1, 2 do
        fiber.create(function()
            queue.identify(session_uuid)
            
            result:put(tube:take(.1, {subqueue = subqueue}))
        end)
    end

    local first = result:get(.2)
    local second = result:get(.2)
    test:ok(first, 'first task was taken')
    test:ok(second, 'second task was taken concurrently')
    if first and second then
        test:isnt(first[1], second[1], 'different tasks were taken')
    else
        test:fail('different tasks were taken')
    end

    test:ok(tube:ack(first[1]), 'first task was acked')
    test:ok(tube:ack(second[1]), 'second task was acked')
end)

test:test('release with delay', function(test)
    test:plan(4)
    local subqueue = 'delay'
    test:ok(tube:put(789, {subqueue = subqueue}), 'task was put')
    test:ok(tube:put(901, {subqueue = subqueue}), 'task was put')

    local task = tube:take(.1, {subqueue = subqueue})
    test:is(task[3], 789, 'first task was taken')

    tube:release(task[1], {delay = .2})
    task = tube:take(.1, {subqueue = subqueue})
    test:is(task[3], 901, 'second task was taken while first is delayed')

    tube:ack(task[1])
    fiber.sleep(.25)
    task = tube:take(.1, {subqueue = subqueue})
    tube:ack(task[1])
end)

test:test('priority', function(test)
    test:plan(4)
    local subqueue = 'priority'
    test:ok(tube:put(670, {subqueue = subqueue, pri = 1}), 'task was put')
    test:ok(tube:put(671, {subqueue = subqueue, pri = 0}), 'task was put')

    local task = tube:take(.1, {subqueue = subqueue})
    test:is(task[3], 671, 'higher priority task was taken first')

    tube:release(task[1])
    task = tube:take(.1, {subqueue = subqueue})
    test:is(task[3], 671, 'released higher priority task remains first')

    tube:ack(task[1])
    task = tube:take(.1, {subqueue = subqueue})
    tube:ack(task[1])
end)

test:test('if_not_exists', function(test)
    test:plan(2)
    local existing = queue.create_tube('subqueue_ine', 'subqueuettl', {
        if_not_exists = true, engine = engine,
    })

    local same = queue.create_tube('subqueue_ine', 'subqueuettl', {
        if_not_exists = true, engine = engine,
    })
    test:is(existing, same, 'existing tube is reused')

    queue.tube.subqueue_ine = nil
    local reloaded = queue.create_tube('subqueue_ine', 'subqueuettl', {
        if_not_exists = true, engine = engine,
    })
    test:isnt(existing, reloaded, 'tube is loaded from existing space')
end)

test:test('read-only mode', function(test)
    test:plan(7)
    tube:put('read_only', {subqueue = 'read_only', delay = .1})

    local ttl_fiber = tube.raw.fiber
    box.cfg{read_only = true}

    test:ok(queue_state.poll(queue_state.states.WAITING, 10),
        'queue state changed to waiting')
    fiber.sleep(.11)
    test:is(ttl_fiber:status(), 'dead', 'background fiber is canceled')
    test:isnil(tube.raw.fiber, 'background fiber object is cleaned')
    if qc.check_version({1, 7}) then
        test:isnil(tube:take(.2, {subqueue = 'read_only'}),
            'delayed task is not moved to ready')
    else
        local ok = pcall(tube.take, tube, .2, {subqueue = 'read_only'})
        test:is(ok, false, 'task cannot be taken while read-only')
    end

    box.cfg{read_only = false}
    test:ok(queue_state.poll(queue_state.states.RUNNING, 10),
        'queue state changed to running')
    test:is(tube.raw.fiber:status(), 'suspended', 'background fiber restarted')

    local task = tube:take(.2, {subqueue = 'read_only'})
    test:ok(task, 'task can be taken after read-write restore')
    tube:ack(task[1])
end)

test:test('TTL after delayed release', function(test)
    test:plan(2)
    local ttl, ttr, delay = 10, 20, 5
    local ttl_tube = queue.create_tube('subqueue_ttl_release', 'subqueuettl', {
        if_not_exists = true, engine = engine,
    })
    ttl_tube:put('task', {subqueue = 'ttl', ttl = ttl, ttr = ttr})
    local task = ttl_tube:take(.1, {subqueue = 'ttl'})
    ttl_tube:release(task[1], {delay = delay})

    task = box.space.subqueue_ttl_release:get(task[1])
    test:is(task.ttl, (ttl + delay) * 1000000, 'TTL includes release delay')
    test:is(task.ttr, ttr * 1000000, 'TTR is unchanged')
end)

test:test('TTL after release without delay', function(test)
    test:plan(2)

    local ttl_tube = queue.create_tube('subqueue_ttl_release_reg', 'subqueuettl', {
        if_not_exists = true, engine = engine,
    })

    local ttl = 0.10
    ttl_tube:put('task', {subqueue = 'reg', ttl = ttl, ttr = 10})

    local task = ttl_tube:take(.1, {subqueue = 'reg'})
    ttl_tube:release(task[1]) -- No delay.

    fiber.sleep(ttl + 0.5)

    test:is(ttl_tube:take(.1, {subqueue = 'reg'}), nil,
        'task must be deleted by TTL after release without delay')

    test:ok(true, 'done')
end)

test:test('tasks by state', function(test)
    test:plan(2)
    local state_tube = queue.create_tube('subqueue_by_state', 'subqueuettl', {
        engine = engine,
    })
    for i = 1, 10 do
        state_tube:put('task_' .. i, {subqueue = tostring(i)})
    end
    for i = 1, 4 do
        state_tube:take(.001, {subqueue = tostring(i)})
    end

    local stats = queue.statistics(state_tube.name)

    test:is(stats.tasks.ready, 6, 'ready task count')
    test:is(stats.tasks.taken, 4, 'taken task count')
end)

test:test('consumer group wakeup', function(test)
    test:plan(10)
    test:ok(tube:put('first', {subqueue = 'first'}), 'put first subqueue task')
    test:ok(tube:put('second', {subqueue = 'second'}), 'put second subqueue task')

    local task = tube:take(.1, {subqueue = 'second'})
    test:ok(task, 'take requested subqueue task')
    test:is(task and task[3], 'second', 'take only requested subqueue task')
    test:ok(not pcall(tube.take, tube, .1), 'take without subqueue is rejected')
    test:ok(tube:ack(task[1]), 'ack subqueue task')

    local result = fiber.channel(1)
    fiber.create(function()
        result:put(tube:take(.2, {subqueue = 'target'}))
    end)
    fiber.sleep(.01)

    test:ok(tube:put('other', {subqueue = 'other'}), 'put other subqueue task')
    test:isnil(result:get(.05), 'other subqueue does not wake target consumer')

    test:ok(tube:put('target', {subqueue = 'target'}), 'put target subqueue task')
    task = result:get(.1)
    test:is(task and task[3], 'target', 'target subqueue wakes matching consumer')
end)

test:test('registry names persist after ack', function(test)
    test:plan(5)
    local t = queue.create_tube('subqueue_reg_del', 'subqueuettl',
        {engine = engine})

    t:put('a', {subqueue = 'x'})
    t:put('b', {subqueue = 'x'})
    t:put('c', {subqueue = 'y'})

    local stats = queue.statistics(t.name)
    test:is(stats.extra.subqueues.x.total, 2, 'two tasks in subqueue x')
    test:is(stats.extra.subqueues.y.total, 1, 'one task in subqueue y')
    test:is(count_subqueues(stats), 2, 'two subqueues registered')

    -- Ack all tasks in x; the subqueue name must remain in the registry
    -- (best-effort, no cleanup on delete) and report zero counts.
    local task = t:take(.001, {subqueue = 'x'})
    t:ack(task[1])
    task = t:take(.001, {subqueue = 'x'})
    t:ack(task[1])

    stats = queue.statistics(t.name)
    test:is(stats.extra.subqueues.x.total, 0,
        'subqueue x reports zero total after all tasks acked')
    test:is(count_subqueues(stats), 2,
        'subqueue names still registered after acks')

    t:drop()
end)

test:test('registry cleared on truncate', function(test)
    test:plan(4)
    local t = queue.create_tube('subqueue_reg_trunc', 'subqueuettl',
        {engine = engine})

    t:put('a', {subqueue = 'x'})
    t:put('b', {subqueue = 'y'})

    local stats = queue.statistics(t.name)
    test:is(count_subqueues(stats), 2, 'two subqueues before truncate')
    test:is(stats.tasks.total, 2, 'two tasks before truncate')

    t:truncate()

    stats = queue.statistics(t.name)
    test:is(count_subqueues(stats), 0, 'no subqueues after truncate')
    test:is(stats.tasks.total, 0, 'no tasks after truncate')

    t:drop()
end)

test:test('registry backfilled on reload', function(test)
    test:plan(4)
    local t = queue.create_tube('subqueue_reg_reload', 'subqueuettl',
        {if_not_exists = true, engine = engine})

    t:put('a', {subqueue = 'r1'})
    t:put('b', {subqueue = 'r2'})

    -- Simulate reload: drop the in-memory tube reference and the registry
    -- space, then recreate from the persisted main space. The driver must
    -- backfill the registry from existing tasks.
    queue.tube.subqueue_reg_reload = nil
    box.space.subqueue_reg_reload_subqueues:drop()
    local reloaded = queue.create_tube('subqueue_reg_reload', 'subqueuettl',
        {if_not_exists = true, engine = engine})

    local stats = queue.statistics(reloaded.name)
    test:is(stats.extra.subqueues.r1.total, 1,
        'subqueue r1 backfilled after reload')
    test:is(stats.extra.subqueues.r2.total, 1,
        'subqueue r2 backfilled after reload')
    test:is(count_subqueues(stats), 2, 'two subqueues backfilled after reload')
    test:is(stats.tasks.total, 2, 'all tasks preserved after reload')

    reloaded:drop()
end)

test:test('registry space is dropped with tube', function(test)
    test:plan(3)
    local t = queue.create_tube('subqueue_reg_drop', 'subqueuettl',
        {engine = engine})

    t:put('a', {subqueue = 'd1'})
    test:ok(box.space.subqueue_reg_drop_subqueues ~= nil,
        'registry space exists while tube is alive')

    t:drop()
    test:isnil(box.space.subqueue_reg_drop,
        'main space dropped with tube')
    test:isnil(box.space.subqueue_reg_drop_subqueues,
        'registry space dropped with tube')
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
