#!/usr/bin/env tarantool
local test = require('tap').test()
test:plan(3)

local test_user = 'test'
local test_pass = '1234'
local test_host = 'localhost'
local test_port = '3388'

local uri = require('uri')
local netbox = require('net.box')

local tnt = require('t.tnt')

tnt.cfg{
    listen = ('%s:%s'):format(test_host, test_port)
    -- listen = uri.format({ host = test_host, service = test_port })
}

local engine = os.getenv('ENGINE') or 'memtx'

local qc = require('queue.compat')

test:test('check for space grants', function(test)
    -- prepare for tests
    local queue = require('queue')
    box.schema.user.create(test_user, { password = test_pass })

    test:plan(5)

    local tube = queue.create_tube('test', 'fifo', { engine = engine })
    tube:put('help');
    local task = tube:take();
    test:is(task[1], 0, 'we can get record')
    tube:release(task[1])

    -- checking without grants
    box.session.su('test')
    local stat, er = pcall(tube.take, tube)
    test:is(stat, false, 'we\'re getting error')
    box.session.su('admin')

    -- checking with grants
    tube:grant('test')
    box.session.su('test')
    local a = tube:take()
    test:is(a[1], 0, 'we aren\'t getting any error')
    local b = tube:take(0.1)
    test:isnil(b, 'we aren\'t getting any error')
    local c = tube:ack(a[1])
    test:is(a[1], 0, 'we aren\'t getting any error')
    box.session.su('admin')

    -- checking double grants
    tube:grant('test')

    box.schema.user.drop(test_user)
    tube:drop()
end)

test:test('check for call grants', function(test)
    -- prepare for tests
    rawset(_G, 'queue', require('queue'))
    box.schema.user.create(test_user, { password = test_pass })

    test:plan(12)

    local tube = queue.create_tube('test', 'fifo', { engine = engine })
    tube:put('help');
    local task = tube:take();
    test:is(task[1], 0, 'we can get record')
    tube:release(task[1])

    -- checking without grants
    box.session.su('test')
    local stat, er = pcall(tube.take, tube)
    test:is(stat, false, 'we\'re getting error')
    box.session.su('admin')

    -- checking with grants
    tube:grant('test')

    box.session.su('test')
    local a = tube:take()
    test:is(a[1], 0, 'we aren\'t getting any error')
    local b = tube:take(0.1)
    test:isnil(b, 'we aren\'t getting any error')
    local c = tube:release(a[1])
    test:is(a[1], 0, 'we aren\'t getting any error')
    box.session.su('admin')

    local nb_connect = netbox.connect
    if nb_connect == nil then
        nb_connect = netbox.new
    end
    local con = nb_connect(
        ('%s:%s@%s:%s'):format(test_user, test_pass, test_host, test_port)
    )
    --[[
    local con = netbox.connect(uri.format({
        login = test_user, password = test_pass,
        host  = test_host, service  = test_port,
    }, true))
    ]]--

    local stat, err = pcall(con.call, con, 'queue.tube.test:take')
    test:is(stat, false, 'we\'re getting error')

    -- granting call
    tube:grant('test', { call = true })

    local qc_arg_unpack = function(arg)
        if qc.check_version({1, 7}) then
            return arg
        end
        return arg and arg[1]
    end

    local a = con:call('queue.tube.test:take')
    test:is(qc_arg_unpack(a[1]), 0, 'we aren\'t getting any error')
    local b = con:call('queue.tube.test:take', qc.pack_args(0.1))
    test:isnil(
        qc_arg_unpack(qc_arg_unpack(b)),
        'we aren\'t getting any error'
    )
    local c = con:call('queue.tube.test:ack',  qc.pack_args(qc_arg_unpack(a[1])))
    test:is(qc_arg_unpack(a[1]), 0, 'we aren\'t getting any error')

    local d = con:call('queue.tube.test:put', {'help'})
    test:is(qc_arg_unpack(d[1]), 0, 'we aren\'t getting any error')

    local e = con:call('queue.statistics')
    test:is(type(qc_arg_unpack(e)), 'table', 'we aren\'t getting any error')

    tube:grant('test', { truncate = true })
    local f = con:call('queue.tube.test:truncate')
    test:isnil(qc_arg_unpack(f), 'we aren\'t getting any error')

    -- check grants again
    tube:grant('test', { call = true })

    rawset(_G, 'queue', nil)
    tube:drop()
end)

test:test('check tube existence', function(test)
    test:plan(14)
    local queue = require('queue')
    test:is(#queue.tube(), 0, 'checking for empty tube list')
    assert(#queue.tube() == 0)

    local tube1 = queue.create_tube('test1', 'fifo', { engine = engine })
    test:is(#queue.tube(), 1, 'checking for not empty tube list')

    local tube2 = queue.create_tube('test2', 'fifo', { engine = engine })
    test:is(#queue.tube(), 2, 'checking for not empty tube list')

    test:is(queue.tube('test1'), true, 'checking for tube existence')
    test:is(queue.tube('test2'), true, 'checking for tube existence')
    test:is(queue.tube('test3'), false, 'checking for tube nonexistence')

    tube2:drop()
    test:is(#queue.tube(), 1, 'checking for not empty tube list')

    test:is(queue.tube('test1'), true, 'checking for tube existence')
    test:is(queue.tube('test2'), false, 'checking for tube nonexistence')
    test:is(queue.tube('test3'), false, 'checking for tube nonexistence')

    tube1:drop()
    test:is(#queue.tube(), 0, 'checking for empty tube list')

    test:is(queue.tube('test1'), false, 'checking for tube nonexistence')
    test:is(queue.tube('test2'), false, 'checking for tube nonexistence')
    test:is(queue.tube('test3'), false, 'checking for tube nonexistence')
end)

tnt.finish()

os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
