#!/usr/bin/env tarantool
local test = require('tap').test()
test:plan(17)

local test_user = 'test'
local test_role = 'test_role'
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

local test_drivers_grant_cases = {
    {
        name = 'fifo',
        queue_type = 'fifo',
    },
    {
        name = 'fifottl',
        queue_type = 'fifottl',
    },
    {
        name = 'utube_default',
        queue_type = 'utube',
        storage_mode = 'default',
    },
    {
        name = 'utube_ready_buffer',
        queue_type = 'utube',
        storage_mode = 'ready_buffer',
    },
    {
        name = 'utubettl_default',
        queue_type = 'utubettl',
        storage_mode = 'default',
    },
    {
        name = 'utubettl_ready_buffer',
        queue_type = 'utubettl',
        storage_mode = 'ready_buffer',
    },
}

for _, tc in pairs(test_drivers_grant_cases) do
    test:test('test drivers grant ' .. tc.name, function(test)
        local queue = require('queue')
        box.schema.user.create(test_user, { password = test_pass })

        test:plan(2)

        local tube_opts = { engine = engine }
        if tc.storage_mode ~= nil and tc.storage_mode ~= 'default' then
            tube_opts.storage_mode = tc.storage_mode
            tube_opts.engine = 'memtx'
        end
        local tube = queue.create_tube('test', tc.queue_type, tube_opts)
        tube:put('help')

        tube:grant(test_user)

        box.session.su(test_user)
        local a = tube:take()
        test:is(a[1], 0, 'we aren\'t getting any error')

        local c = tube:ack(a[1])
        test:is(c[1], 0, 'we aren\'t getting any error')

        box.session.su('admin')

        box.schema.user.drop(test_user)
        tube:drop()
    end)

    test:test('test drivers grant_role ' .. tc.name, function(test)
        local queue = require('queue')
        box.schema.user.create(test_user, { password = test_pass })
        box.schema.role.create(test_role)
        box.schema.user.grant(test_user, test_role, nil, nil, { if_not_exists = true })

        test:plan(2)
        local tube_opts = { engine = engine }
        if tc.storage_mode ~= nil and tc.storage_mode ~= 'default' then
            tube_opts.storage_mode = tc.storage_mode
            tube_opts.engine = 'memtx'
        end

        local tube_name = 'test_' .. tc.name .. '_role'
        local tube = queue.create_tube(tube_name, tc.queue_type, tube_opts)
        tube:put('help')

        tube:grant_role(test_role)

        box.session.su(test_user, function()
            local a = tube:take()
            test:is(a[1], 0, 'take works via role grants')

            local c = tube:ack(a[1])
            test:is(c[1], 0, 'ack works via role grants')
        end)

        tube:drop()
        box.schema.user.drop(test_user)
        box.schema.role.drop(test_role)
    end)
end

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

test:test('check for space grants via role', function(test)
    local queue = require('queue')
    box.schema.user.create(test_user, { password = test_pass })
    box.schema.role.create(test_role)
    box.schema.user.grant(test_user, test_role, nil, nil, { if_not_exists = true })

    test:plan(5)

    local tube = queue.create_tube('test', 'fifo', { engine = engine })
    tube:put('help')
    local task = tube:take()
    test:is(task[1], 0, 'admin can take record')
    tube:release(task[1])

    -- Without grants.
    box.session.su(test_user)
    local stat = pcall(tube.take, tube)
    test:is(stat, false, 'no access without grants')
    box.session.su('admin')

    -- With role grants.
    tube:grant_role(test_role)

    box.session.su(test_user)
    local a = tube:take()
    test:is(a[1], 0, 'take works with role grants')
    local b = tube:take(0.1)
    test:isnil(b, 'take timeout works with role grants')
    local c = tube:ack(a[1])
    test:is(c[1], 0, 'ack works with role grants')
    box.session.su('admin')

    tube:drop()
    box.schema.user.drop(test_user)
    box.schema.role.drop(test_role)
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
    box.schema.user.drop(test_user)
end)

test:test('check for call grants via role', function(test)
    rawset(_G, 'queue', require('queue'))
    box.schema.user.create(test_user, { password = test_pass })
    box.schema.role.create(test_role)
    box.schema.user.grant(test_user, test_role, nil, nil, { if_not_exists = true })

    test:plan(9)

    local tube = queue.create_tube('test', 'fifo', { engine = engine })
    tube:put('help')
    local task = tube:take()
    test:is(task[1], 0, 'admin can take record')
    tube:release(task[1])

    -- Without call grants.
    local nb_connect = netbox.connect or netbox.new
    local con = nb_connect(('%s:%s@%s:%s'):format(test_user, test_pass, test_host, test_port))

    local stat = pcall(con.call, con, 'queue.tube.test:take')
    test:is(stat, false, 'no execute on tube function without call grants')

    -- Grant call via role.
    tube:grant_role(test_role, { call = true })

    local id = con:call('queue.identify')
    test:ok(id ~= nil, 'queue.identify via net.box works with role call grants')

    local qc_arg_unpack = function(arg)
        if qc.check_version({1, 7}) then
            return arg
        end
        return arg and arg[1]
    end

    local a = con:call('queue.tube.test:take')
    test:is(qc_arg_unpack(a[1]), 0, 'take via net.box works with role call grants')

    local b = con:call('queue.tube.test:take', qc.pack_args(0.1))
    test:isnil(qc_arg_unpack(qc_arg_unpack(b)), 'take timeout via net.box works')

    local c = con:call('queue.tube.test:ack', qc.pack_args(qc_arg_unpack(a[1])))
    test:is(qc_arg_unpack(c[1]), 0, 'ack via net.box works')

    local d = con:call('queue.tube.test:put', {'help'})
    test:is(qc_arg_unpack(d[1]) >= 0, true, 'put via net.box works')

    local e = con:call('queue.statistics')
    test:is(type(qc_arg_unpack(e)), 'table', 'queue.statistics via net.box works')

    tube:grant_role(test_role, { truncate = true })
    local f = con:call('queue.tube.test:truncate')
    test:isnil(qc_arg_unpack(f), 'truncate via net.box works')

    -- Check double grants.
    tube:grant_role(test_role, { call = true })

    con:close()
    rawset(_G, 'queue', nil)

    tube:drop()
    box.schema.user.drop(test_user)
    box.schema.role.drop(test_role)
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
