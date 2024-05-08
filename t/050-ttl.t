#!/usr/bin/env tarantool
local fiber = require('fiber')

local test  = require('tap').test()
test:plan(7)

local queue = require('queue')

local engine = os.getenv('ENGINE') or 'memtx'

local tnt = require('t.tnt')
tnt.cfg{
    wal_mode = (engine == 'memtx' and 'none' or nil)
}

local ttl = 0.1

test:ok(queue, 'queue is loaded')

local function test_take_after_ttl(test, tube, ttl)
    local attempts, max_attempts = 0, 2
    local before = fiber.time64()
    local after = before

    while after - before < ttl * 1000000 and attempts < max_attempts do
        attempts = attempts + 1
        fiber.sleep(ttl)
        after = fiber.time64()
    end

    if after - before < ttl * 1000000 then
        test:fail('failed to sleep ttl, is system clock changed?')
    else
        test:isnil(tube:take(.1), 'no task is taken')
    end
end

test:test('one message per queue ffttl', function (test)
    test:plan(20)
    local tube = queue.create_tube('ompq_ffttl', 'fifottl', { engine = engine })
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})

        test_take_after_ttl(test, tube, ttl)
    end
end)

test:test('one message per queue utttl', function (test)
    test:plan(20)
    local tube = queue.create_tube('ompq_utttl', 'utubettl', { engine = engine })
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})

        test_take_after_ttl(test, tube, ttl)
    end
end)

test:test('one message per queue utttl_ready', function (test)
    if engine == 'vinyl' then
        return
    end

    test:plan(20)
    local tube = queue.create_tube('ompq_utttl_ready', 'utubettl',
        { engine = engine, storage_mode = queue.driver.utubettl.STORAGE_MODE_READY_BUFFER })
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})

        test_take_after_ttl(test, tube, ttl)
    end
end)

test:test('many messages, one queue ffttl', function (test)
    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_ffttl_' .. i, 'fifottl', { engine = engine })
        tube:put('mmpq_' .. i, {ttl=ttl})

        test_take_after_ttl(test, tube, ttl)
    end
end)

test:test('many messages, one queue utttl', function (test)
    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_utttl_' .. i, 'utubettl', { engine = engine })
        tube:put('mmpq_' .. i, {ttl=ttl})

        test_take_after_ttl(test, tube, ttl)
    end
end)

test:test('many messages, one queue utttl_ready', function (test)
    if engine == 'vinyl' then
        return
    end

    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_utttl_ready_' .. i, 'utubettl',
            { engine = engine, storage_mode = queue.driver.utubettl.STORAGE_MODE_READY_BUFFER })
        tube:put('mmpq_' .. i, {ttl=ttl})

        test_take_after_ttl(test, tube, ttl)
    end
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
