#!/usr/bin/env tarantool

local test = require('tap').test('Version parsing module test')
local version = require('queue.abstract.version')

test:plan(4)

test:is_deeply(version.parse('1.5'), { major = 1, minor = 5, patch = ''}, '1.5')
test:is_deeply(version.parse('1.6.8'), { major = 1, minor = 6, patch = '8'}, '1.6.8')
test:is_deeply(version.parse('1.7.2-1-g92ed6c4'), { major = 1, minor = 7, patch = '2-1-g92ed6c4'}, '1.7.2-1-g92ed6c4')
test:is_deeply(version.parse('1.11.abcdef'), { major = 1, minor = 11, patch = 'abcdef'}, '1.11.abcdef')

test:check()