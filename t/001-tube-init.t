#!/usr/bin/env tarantool
local yaml  = require('yaml')
local fiber = require('fiber')

local tnt  = require('t.tnt')

local test = require('tap').test()
test:plan(1)

local engine = os.getenv('ENGINE') or 'memtx'

local mock_tube = { create_space = function() end, new = function() end }

test:test('test queue mock addition', function(test)
    test:plan(2)

    local queue = require('queue')
    queue.register_driver('mock', mock_tube)
    test:is(queue.driver.mock, mock_tube)
    tnt.cfg{}

    test:is(queue.driver.mock, mock_tube)
end)

tnt.finish()
os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua :
