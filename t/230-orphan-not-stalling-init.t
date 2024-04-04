#!/usr/bin/env tarantool

local test  = require('tap').test('')
local queue = require('queue')
local tnt   = require('t.tnt')
local fio   = require('fio')
local fiber = require('fiber')

rawset(_G, 'queue', require('queue'))

local qc = require('queue.compat')
if not qc.check_version({2, 10, 0}) then
    require('log').info('Tests skipped, tarantool version < 2.10.0 ' ..
        'does not support the lazy init')
    return
end

local snapdir_optname = qc.snapdir_optname
local logger_optname  = qc.logger_optname

test:plan(1)

test:test('Check orphan mode not stalling queue', function(test)
    test:plan(4)
    local engine = os.getenv('ENGINE') or 'memtx'
    tnt.cluster.cfg{}

    local dir_replica = fio.tempdir()
    local cmd_replica = {
        arg[-1],
        '-e',
        [[
        box.cfg {
            replication = {
                'replicator:password@127.0.0.1:3399',
                'replicator:password@127.0.0.1:3398',
            },
            listen = '127.0.0.1:3396',
            wal_dir = ']] .. dir_replica .. '\'' ..
            ',' .. snapdir_optname() .. ' = \'' .. dir_replica .. '\'' ..
            ',' .. logger_optname() .. ' = \'' ..
            fio.pathjoin(dir_replica, 'tarantool.log') .. '\'' ..
        '}'
    }

    replica = require('popen').new(cmd_replica, {
        stdin = 'devnull',
        stdout = 'devnull',
        stderr = 'devnull',
    })

    local attempts = 0
    -- Wait for replica to connect.
    while box.info.replication[3] == nil or
        box.info.replication[3].downstream.status ~= 'follow' do

        attempts = attempts + 1
        if attempts == 30 then
            error('wait for replica connection')
        end
        fiber.sleep(0.1)
    end

    local conn = require('net.box').connect('127.0.0.1:3396')

    conn:eval([[
        box.cfg{
            replication = {
                'replicator:password@127.0.0.1:3399',
                'replicator:password@127.0.0.1:3398',
                'replicator:password@127.0.0.1:3396',
            },
            listen = '127.0.0.1:3397',
            replication_connect_quorum = 4,
        }
    ]])

    conn:eval('rawset(_G, "queue", require("queue"))')

    test:is(conn:call('queue.state'), 'INIT', 'check queue state')
    test:is(conn:call('box.info').ro, true, 'check read only')
    test:is(conn:call('box.info').ro_reason, 'orphan', 'check ro reason')

    conn:eval('box.cfg{replication_connect_quorum = 2}')

    local attempts = 0
    while conn:call('queue.state') ~= 'RUNNING' and attempts < 50 do
        fiber.sleep(0.1)
        attempts = attempts + 1
    end
    test:is(conn:call('queue.state'), 'RUNNING', 'check queue state after orphan')
end)

rawset(_G, 'queue', nil)
tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
