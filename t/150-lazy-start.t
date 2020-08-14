#!/usr/bin/env tarantool
local tap = require('tap')
local tnt = require('t.tnt')

local test = tap.test('test driver register')
test:plan(3)

local function check_lazy_start()
    -- Needed for bootstrap
    tnt.cfg{}

    tnt.cfg{read_only = true}
    local queue = require('queue')

    local err_msg = 'Please configure box.cfg{} in read/write mode first'
    local res, err = pcall(function() queue.stats() end)
    local check = not res and string.match(err,err_msg) ~= nil
    test:ok(check, 'check queue delayed start')

    tnt.cfg({read_only = true})
    res, err = pcall(function() queue.stats() end)
    check = not res and string.match(err, err_msg) ~= nil
    test:ok(check, 'check box reconfiguration with read_only = true')

    tnt.cfg({read_only = false})
    res = pcall(function() queue.stats() end)
    test:ok(res, 'queue has been started')
end

check_lazy_start()

tnt.finish()
os.exit(test:check() and 0 or 1)
