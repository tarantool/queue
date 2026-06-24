#!/usr/bin/env tarantool
local test = require('tap').test()
test:plan(6)

local test_pass = '1234'
local test_host = 'localhost'
local test_port = '3388'

local netbox = require('net.box')
local tnt = require('t.tnt')
local qc = require('queue.compat')

tnt.cfg{
    listen = ('%s:%s'):format(test_host, test_port),
}

local engine = os.getenv('ENGINE') or 'memtx'

test:test('grant_role does not affect direct user grant', function(test)
    local queue = require('queue')

    local role = 'test_role_mix'
    local user = 'test_user_mix'

    box.schema.role.create(role)
    box.schema.user.create(user, { password = test_pass })
    box.schema.user.grant(user, role, nil, nil, { if_not_exists = true })

    test:plan(4)

    local tube = queue.create_tube('test_mix_grants', 'fifo', { engine = engine })
    tube:put('a')

    -- Only role grants.
    tube:grant_role(role)

    box.session.su(user)
    local t = tube:take()
    test:ok(t ~= nil, 'user can take via role')
    tube:ack(t[1])
    box.session.su('admin')

    -- Add direct user grant and ensure still works.
    tube:put('b')
    tube:grant(user)

    box.session.su(user)
    local t2 = tube:take()
    test:ok(t2 ~= nil, 'user can take after direct user grant added')
    tube:ack(t2[1])
    box.session.su('admin')

    -- Grant again should not break anything.
    tube:grant_role(role)
    tube:grant(user)

    tube:put('c')
    box.session.su(user)
    local t3 = tube:take()
    test:ok(t3 ~= nil, 'user can take after repeated grants')
    local a3 = tube:ack(t3[1])
    test:ok(a3 ~= nil, 'user can ack after repeated grants')
    box.session.su('admin')

    tube:drop()
    box.schema.user.drop(user)
    box.schema.role.drop(role)
end)

test:test('revoking role does not remove direct user grants', function(test)
    local queue = require('queue')

    local role = 'test_role_revoke'
    local user = 'test_user_revoke'

    box.schema.role.create(role)
    box.schema.user.create(user, { password = test_pass })
    box.schema.user.grant(user, role, nil, nil, { if_not_exists = true })

    test:plan(4)

    local tube = queue.create_tube('test_revoke_mix', 'fifo', { engine = engine })
    tube:put('a')

    -- Give both grants.
    tube:grant_role(role)
    tube:grant(user)

    -- Revoke role from user.
    box.schema.user.revoke(user, role, nil, nil, { if_not_exists = true })

    -- User should still have access due to direct grant.
    box.session.su(user)
    local t = tube:take()
    test:ok(t ~= nil, 'user can take after role revoked (direct grant remains)')
    local a = tube:ack(t[1])
    test:ok(a ~= nil, 'user can ack after role revoked')
    box.session.su('admin')

    -- Revoke direct grants.
    tube:put('b')
    box.session.su(user)
    local t2 = tube:take()
    test:ok(t2 ~= nil, 'still can take (direct grant still present)')
    tube:ack(t2[1])
    box.session.su('admin')

    test:ok(true, 'revoke role test completed')

    tube:drop()
    box.schema.user.drop(user)
    box.schema.role.drop(role)
end)

test:test('grant_role: multiple users inherit access from one role', function(test)
    rawset(_G, 'queue', require('queue'))
    local queue = _G.queue

    local role = 'test_role'
    local u1 = 'test_u1'
    local u2 = 'test_u2'
    local pass = test_pass

    box.schema.role.create(role)
    box.schema.user.create(u1, { password = pass })
    box.schema.user.create(u2, { password = pass })

    box.schema.user.grant(u1, role, nil, nil, { if_not_exists = true })
    box.schema.user.grant(u2, role, nil, nil, { if_not_exists = true })

    test:plan(7)

    local tube = queue.create_tube('test_multi_role', 'fifo', { engine = engine })
    tube:put('a')
    tube:put('b')

    tube:grant_role(role, { call = true })

    local c1 = netbox.connect(('%s:%s'):format(test_host, test_port), {
        user = u1, password = pass
    })
    local c2 = netbox.connect(('%s:%s'):format(test_host, test_port), {
        user = u2, password = pass
    })

    -- First user takes and acks first task.
    local t1 = c1:call('queue.tube.test_multi_role:take')
    test:is(t1[1], 0, 'u1 can take')
    local a1 = c1:call('queue.tube.test_multi_role:ack', {t1[1]})
    test:is(a1[1], 0, 'u1 can ack')

    -- Second user takes and acks second task.
    local t2 = c2:call('queue.tube.test_multi_role:take')
    test:is(t2[1], 1, 'u2 can take next task')
    local a2 = c2:call('queue.tube.test_multi_role:ack', {t2[1]})
    test:is(a2[1], 1, 'u2 can ack')

    -- First user puts from and takes again.
    local p = c1:call('queue.tube.test_multi_role:put', {'c'})
    test:is(p[3], 'c', 'u1 can put')

    local t3 = c1:call('queue.tube.test_multi_role:take')
    test:is(t3[3], 'c', 'u1 can take again after new put')
    local a3 = c1:call('queue.tube.test_multi_role:ack', {t3[1]})
    test:is(a3[3], 'c', 'u1 can ack again')

    c1:close()
    c2:close()
    rawset(_G, 'queue', nil)

    tube:drop()
    box.schema.user.drop(u1)
    box.schema.user.drop(u2)
    box.schema.role.drop(role)
end)

test:test('grant_role: one role can access multiple tubes', function(test)
    local queue = require('queue')

    local role = 'test_role'
    local user = 'test_user'

    box.schema.role.create(role)
    box.schema.user.create(user, { password = test_pass })
    box.schema.user.grant(user, role, nil, nil, { if_not_exists = true })

    test:plan(4)

    local t1 = queue.create_tube('test_role_t1', 'fifo', { engine = engine })
    local t2 = queue.create_tube('test_role_t2', 'fifo', { engine = engine })

    t1:put('x')
    t2:put('y')

    t1:grant_role(role)
    t2:grant_role(role)

    box.session.su(user, function()
        local a = t1:take()
        test:is(a[3], 'x', 'user can take from tube1')
        t1:ack(a[1])

        local b = t2:take()
        test:is(b[3], 'y', 'user can take from tube2')
        t2:ack(b[1])

        test:ok(t1:put('z') ~= nil, 'user can put to tube1')
        test:ok(t2:put('w') ~= nil, 'user can put to tube2')
    end)

    t1:drop()
    t2:drop()
    box.schema.user.drop(user)
    box.schema.role.drop(role)
end)

test:test('grant_role call: multiple net.box users can call tube functions', function(test)
    rawset(_G, 'queue', require('queue'))
    local queue = _G.queue

    local role = 'test_role_call_multi'
    local u1 = 'test_call_u1'
    local u2 = 'test_call_u2'

    box.schema.role.create(role)
    box.schema.user.create(u1, { password = test_pass })
    box.schema.user.create(u2, { password = test_pass })
    box.schema.user.grant(u1, role, nil, nil, { if_not_exists = true })
    box.schema.user.grant(u2, role, nil, nil, { if_not_exists = true })

    test:plan(4)

    local tube = queue.create_tube('test_call_multi', 'fifo', { engine = engine })
    tube:put('a')
    tube:put('b')

    tube:grant_role(role, { call = true })

    local nb_connect = netbox.connect or netbox.new
    local c1 = nb_connect(('%s:%s@%s:%s'):format(u1, test_pass, test_host, test_port))
    local c2 = nb_connect(('%s:%s@%s:%s'):format(u2, test_pass, test_host, test_port))

    local t1 = c1:call('queue.tube.test_call_multi:take')
    test:is(t1[1], 0, 'u1 can call take')
    local a1 = c1:call('queue.tube.test_call_multi:ack', qc.pack_args(t1[1]))
    test:is(a1[1], 0, 'u1 can call ack')

    local t2 = c2:call('queue.tube.test_call_multi:take')
    test:is(t2[1], 1, 'u2 can call take')
    local a2 = c2:call('queue.tube.test_call_multi:ack', qc.pack_args(t2[1]))
    test:is(a2[1], 1, 'u2 can call ack')

    c1:close()
    c2:close()
    rawset(_G, 'queue', nil)

    tube:drop()
    box.schema.user.drop(u1)
    box.schema.user.drop(u2)
    box.schema.role.drop(role)
end)

test:test('grant_role: utube ready_buffer works for multiple users', function(test)
    local queue = require('queue')

    local role = 'test_role_utube_rb'
    local u1 = 'test_utube_u1'
    local u2 = 'test_utube_u2'

    box.schema.role.create(role)
    box.schema.user.create(u1, { password = test_pass })
    box.schema.user.create(u2, { password = test_pass })
    box.schema.user.grant(u1, role, nil, nil, { if_not_exists = true })
    box.schema.user.grant(u2, role, nil, nil, { if_not_exists = true })

    test:plan(4)

    local tube = queue.create_tube('test_utube_rb', 'utube', {
        engine = 'memtx',
        storage_mode = 'ready_buffer',
    })

    tube:grant_role(role)

    tube:put('d1', { utube = 'k1' })
    tube:put('d2', { utube = 'k2' })

    box.session.su(u1, function()
        local t = tube:take()
        test:ok(t ~= nil, 'u1 can take')
        tube:ack(t[1])
    end)

    box.session.su(u2, function()
        local t = tube:take()
        test:ok(t ~= nil, 'u2 can take')
        tube:ack(t[1])
    end)

    box.session.su(u1, function()
        test:isnil(tube:take(0.01), 'no more tasks for u1')
    end)
    box.session.su(u2, function()
        test:isnil(tube:take(0.01), 'no more tasks for u2')
    end)

    tube:drop()
    box.schema.user.drop(u1)
    box.schema.user.drop(u2)
    box.schema.role.drop(role)
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
