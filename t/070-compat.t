#!/usr/bin/env tarantool

local test  = require('tap').test()
test:plan(3)

local qcompat = require('queue.compat')

test:ok(qcompat, 'queue compatibility layer exists')

test:test('*_version', function(test)
    test:plan(9)

    local split_version = qcompat.split_version
    local check_version = qcompat.check_version

    test:is_deeply(split_version("1.6.8-173"), {"1", "6", "8", "173"},
                   "check split_version 1")
    test:is_deeply(split_version("1.7.1-0"), {"1", "7", "1", "0"},
                   "check split_version 2")
    test:is_deeply(split_version("1.7.1"), {"1", "7", "1"},
                   "check split_version 3")

    test:ok(check_version({1, 7, 1}, "1.8.1-0"),
            "check supported version")
    test:ok(check_version({1, 7, 1}, "1.7.1-0"),
            "check supported version")
    test:ok(check_version({1, 7, 1}, "1.7.1-1"),
            "check supported version")
    test:ok(not check_version({1, 7, 1}, "1.6.9"),
            "check unsupported version")
    test:ok(not check_version({1, 7, 1}, "1.6.9-100"),
            "check unsupported version")
    test:ok(not check_version({1, 7, 1}, "1.6.9-100"),
            "check unsupported version")
end)

test:test("check compatibility names", function(test)
    test:plan(7)

    local vinyl_name = qcompat.vinyl_name
    local num_name   = qcompat.num_type
    local str_name   = qcompat.str_type

    test:is(vinyl_name("1.7.1-168"), "vinyl",    "check new name (vinyl)")
    test:is(num_name("1.7.2-1"),     "unsigned", "check new name (unsigned)")
    test:is(num_name("1.6.9-168"),   "num",      "check old name (num)")
    test:is(num_name("1.7.1-168"),   "num",      "check old name (num)")
    test:is(str_name("1.7.2-1"),     "string",   "check new name (string)")
    test:is(str_name("1.6.9-168"),   "str",      "check old name (str)")
    test:is(str_name("1.7.1-168"),   "str",      "check old name (str)")
end)

os.exit(test:check() == true and 0 or -1)
-- vim: set ft=lua:
