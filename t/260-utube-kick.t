#!/usr/bin/env tarantool
local test = (require('tap')).test()
test:plan(2)

local tnt = require('t.tnt')
tnt.cfg{}

local engine = os.getenv('ENGINE') or 'memtx'

local queue = require('queue')

-- The 'ready_buffer' storage mode is not supported by the vinyl engine.
test:test('kick with the ready buffer', function(test)
    if engine == 'vinyl' then
        test:plan(1)
        test:ok(true, 'skipped for vinyl')
        return
    end
    test:plan(4)

    local buffer_name = 'kick_ready_buffer'

    local tube = queue.create_tube(buffer_name, 'utube', {
        engine = engine,
        storage_mode = queue.driver.utube.STORAGE_MODE_READY_BUFFER
    })
    local buffer = box.space[buffer_name .. '_ready_buffer']

    -- A kicked task must be returned to the ready buffer, otherwise it stays
    -- READY but is never seen by take().
    local task = tube:put('single', {utube = 'u'})
    tube:bury(task[1])
    test:is(tube:kick(1), 1, 'the buried task is kicked')

    local function select_buffer(...)
        local rows = {}
        for _, tuple in ipairs(buffer:select(...)) do
            table.insert(rows, tuple:totable())
        end
        return rows
    end

    test:is_deeply(select_buffer(), {{task[1], 'u'}},
        'the kicked task is in the ready buffer under its utube name')

    local taken = tube:take(0)
    test:is(taken and taken[1], task[1], 'the kicked task can be taken')
    if taken ~= nil then
        tube:ack(taken[1])
    else
        tube:truncate()
    end

    -- A kicked task that displaces a younger task of the same utube must be
    -- written to the buffer with the utube name, not with its status.
    local first  = tube:put('first',  {utube = 'v'})
    local second = tube:put('second', {utube = 'v'})
    tube:take(0)             -- Takes 'first', drops it from the buffer.
    tube:bury(first[1])      -- Buries it, 'second' takes its buffer slot.
    tube:kick(1)             -- Kicks 'first' back, it displaces 'second'.

    local rows = {}
    for _, tuple in ipairs(buffer.index.utube:select({'v'})) do
        table.insert(rows, tuple:totable())
    end
    test:is_deeply(rows, {{first[1], 'v'}},
        'the displacing task keeps the utube name in the buffer')
end)

test:test('kick does not leak a transaction', function(test)
    test:plan(1)

    local tube = queue.create_tube('kick_txn', 'utube', {engine = engine})

    tube:kick(1)  -- Nothing is buried, kick returns early.

    local leaked = box.is_in_txn()
    if leaked then
        box.rollback()
    end
    test:ok(not leaked, 'kick over an empty tube leaves no open transaction')
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
