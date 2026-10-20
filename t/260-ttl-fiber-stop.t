#!/usr/bin/env tarantool
local fiber = require('fiber')

local test = require('tap').test('ttl fiber stop')
local queue = require('queue')
local tnt = require('t.tnt')
tnt.cfg{}

-- gh-262: stop() blocked forever (fifottl) or left the ttl fiber running
-- (utubettl) while the instance was rw, and drop() leaked the fiber.
local drivers = {'fifottl', 'utubettl', 'limfifottl'}
test:plan(#drivers)

local function fibers_named(name)
    local count = 0
    for _, info in pairs(fiber.info()) do
        if info.name == name then
            count = count + 1
        end
    end
    return count
end

-- Runs fn in a separate fiber and waits for it up to timeout seconds.
local function finishes(fn, timeout)
    local done = false
    fiber.create(function()
        fn()
        done = true
    end)
    local deadline = fiber.time() + timeout
    while not done and fiber.time() < deadline do
        fiber.sleep(0.01)
    end
    return done
end

for _, driver in ipairs(drivers) do
    test:test(driver, function(test)
        test:plan(9)
        local fiber_name = driver == 'limfifottl' and 'fifottl' or driver
        local before = fibers_named(fiber_name)
        local tube = queue.create_tube(driver .. '_stop', driver)
        test:is(fibers_named(fiber_name), before + 1, 'ttl fiber is started')

        local ttl_fiber = tube.raw.fiber
        test:ok(finishes(function() tube.raw:stop() end, 1),
            'stop() returns in rw mode')
        test:is(ttl_fiber:status(), 'dead', 'ttl fiber is terminated by stop()')
        test:isnil(tube.raw.fiber, 'ttl fiber is unregistered by stop()')
        test:is(fibers_named(fiber_name), before, 'no ttl fibers left after stop()')

        tube.raw:start()
        test:is(fibers_named(fiber_name), before + 1, 'start() creates one ttl fiber')
        tube:put('data', {ttl = 0.1, utube = 'utube'})
        fiber.sleep(0.3)
        test:is(tube.raw.space:len(), 0, 'ttl is processed after start()')

        ttl_fiber = tube.raw.fiber
        test:ok(finishes(function() tube:drop() end, 1), 'drop() returns in rw mode')
        test:is(fibers_named(fiber_name), before, 'no ttl fibers leaked by drop()')
    end)
end

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
