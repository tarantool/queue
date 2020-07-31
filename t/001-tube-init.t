#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local tnt  = require('t.tnt')

local test = require('tap').test()
test:plan(1)

local engine = os.getenv('ENGINE') or 'memtx'

local mock_tube = { create_space = function() end, new = function() end }

test:test('test queue mock addition', function(test)
    test:plan(3)

    local queue = require('queue')
    queue.register_driver('mock', mock_tube)
    test:is(queue.driver.mock, mock_tube)

    local res, err = pcall(queue.register_driver, 'mock', mock_tube)
    local check = not res and
        string.match(err, 'overriding registered driver') ~= nil
    test:ok(check, 'check a driver override failure')

    tnt.cfg{}

    test:is(queue.driver.mock, mock_tube)
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
