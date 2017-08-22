#!/usr/bin/env tarantool
local fiber = require('fiber')

local test  = require('tap').test()
test:plan(5)

local queue = require('queue')

local tnt = require('t.tnt')
tnt.cfg{
    wal_mode = 'none'
}

local engine = os.getenv('ENGINE') or 'memtx'

local ttl = 0.1

test:ok(queue, 'queue is loaded')

test:test('one message per queue ffttl', function (test)
    test:plan(20)
    local tube = queue.create_tube('ompq_ffttl', 'fifottl', { engine = engine })
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

test:test('one message per queue utttl', function (test)
    test:plan(20)
    local tube = queue.create_tube('ompq_utttl', 'utubettl', { engine = engine })
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

test:test('many messages, one queue ffttl', function (test)
    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_ffttl_' .. i, 'fifottl', { engine = engine })
        tube:put('mmpq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

test:test('many messages, one queue utttl', function (test)
    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_utttl_' .. i, 'utubettl', { engine = engine })
        tube:put('mmpq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
