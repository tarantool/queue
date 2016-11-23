#!/usr/bin/env tarantool

local test = require('tap').test('Unsigned integet type selection')
local num = require('queue.abstract.num')

test:plan(5)

test:is(num.get_num_or_unsigned('1.5'), 'num', '1.5')
test:is(num.get_num_or_unsigned('1.6.8'), 'num', '1.6.8')
test:is(num.get_num_or_unsigned('1.7.2-1-g92ed6c4'), 'unsigned', '1.7.2-1-g92ed6c4')
test:is(num.get_num_or_unsigned('1.11'), 'unsigned', '1.11. Alphabetical sorting and comparison will work wrong')
test:is(num.get_num_or_unsigned('2'), 'unsigned', '2')

test:check()