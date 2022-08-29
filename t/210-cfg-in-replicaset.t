#!/usr/bin/env tarantool

local test  = require('tap').test('')
local queue = require('queue')

rawset(_G, 'queue', require('queue'))

test:plan(2)

test:test('Check in_replicaset switches', function(test)
    test:plan(4)
    local engine = os.getenv('ENGINE') or 'memtx'

    box.cfg({})

    local status, err = pcall(queue.cfg, {in_replicaset = true})
    test:ok(status, 'in_replicaset = true switched')
    local status, _ = pcall(queue.cfg, {in_replicaset = false})
    test:ok(status, 'in_replicaset = false switched')
    local status, _ = pcall(queue.cfg, {in_replicaset = true})
    test:ok(status, 'in_replicaset = true switched')
    local status, _ = pcall(queue.cfg, {in_replicaset = false})
    test:ok(status, 'in_replicaset = false switched')
end)

test:test('Check error create temporary tube', function(test)
    test:plan(2)
    local engine = os.getenv('ENGINE') or 'memtx'

    box.cfg({})

    queue.cfg{ttr = 0.5, in_replicaset = true}
    local opts = {temporary = true, engine = engine}
    local status, err = pcall(queue.create_tube, 'test', 'fifo', opts)
    test:is(status, false, 'test tube should not be created')
    local founded = string.find(err,
        'Cannot create temporary tube in replicaset mode')
    test:ok(founded, 'unexpected error')
end)

rawset(_G, 'queue', nil)
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
