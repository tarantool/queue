#!/usr/bin/env tarantool
local fiber = require('fiber')

local test  = require('tap').test()
test:plan(6)

local tnt  = require 't.tnt'
tnt.cfg{
    wal_mode = 'none'
}

local ttl = 0.1

local queue = require('queue')
test:ok(queue.start(), 'queue.start()')
test:ok(queue, 'queue is loaded')


test:test('one message per queue ffttl', function (test)
    test:plan(20)
    local tube = queue.create_tube('ompq_ffttl', 'fifottl')
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

test:test('one message per queue utttl', function (test)
    test:plan(20)
    local tube = queue.create_tube('ompq_utttl', 'utubettl')
    for i = 1, 20 do
        tube:put('ompq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

test:test('many messages, one queue ffttl', function (test)
    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_ffttl_' .. i, 'fifottl')
        tube:put('mmpq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

test:test('many messages, one queue utttl', function (test)
    test:plan(20)
    for i = 1, 20 do
        local tube = queue.create_tube('mmpq_utttl_' .. i, 'utubettl')
        tube:put('mmpq_' .. i, {ttl=ttl})
        fiber.sleep(ttl)
        test:is(#{tube:take(.1)}, 0, 'no task is taken')
    end
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
