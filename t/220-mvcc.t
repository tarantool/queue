#!/usr/bin/env tarantool

local qc = require('queue.compat')
local log   = require('log')
if not qc.check_version({2, 6, 1}) then
    log.info('Tests skipped, tarantool version < 2.6.1')
    return
end

local fiber = require('fiber')

local test  = require('tap').test()
test:plan(6)

local queue = require('queue')

local tnt   = require('t.tnt')
tnt.cfg{memtx_use_mvcc_engine = true}

local engine = 'memtx'

test:ok(rawget(box, 'space'), 'box started')
test:ok(queue, 'queue is loaded')

local tube = queue.create_tube('test', 'fifo', { engine = engine, temporary = false})
test:ok(tube, 'test tube created')
test:is(tube.name, 'test', 'tube.name')
test:is(tube.type, 'fifo', 'tube.type')

--- That test checks that https://github.com/tarantool/queue/pull/211 is fixed.
-- Previously trying to make parallel 'put' or 'take' calls failed with mvcc enabled.
test:test('concurent put and take with mvcc', function(test)
    test:plan(6)
    -- channels are used to wait for fibers to finish
    -- and check results of the 'take'/'put'.
    local channel_put = fiber.channel(2)
    test:ok(channel_put, 'channel created')
    local channel_take = fiber.channel(2)
    test:ok(channel_take, 'channel created')

    for i = 1, 2 do
        fiber.create(function(i)
            local err = pcall(tube.put, tube, i)
            channel_put:put(err)
        end, i)
    end

    for i = 1, 2 do
        local res = channel_put:get(1)
        test:ok(res, 'task ' .. i .. ' was put')
    end

    for i = 1, 2 do
        fiber.create(function(i)
           local err = pcall(tube.take, tube)
            channel_take:put(err)
        end, i)
    end

    for i = 1, 2 do
        local res = channel_take:get()
        test:ok(res, 'task ' .. i .. ' was taken')
    end
end)

tnt.finish()
os.exit(test:check() and 0 or 1)

-- vim: set ft=lua:
