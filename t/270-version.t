#!/usr/bin/env tarantool
local tap = require('tap')
local tnt = require('t.tnt')

local test = tap.test('module version')
test:plan(1)

-- The instance is writable before the module is loaded, so the queue
-- starts at once, without the "lazy start".
tnt.cfg{}
local queue = require('queue')

test:is(queue._VERSION, require('queue.version'),
        'queue._VERSION after start on a writable instance')

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
