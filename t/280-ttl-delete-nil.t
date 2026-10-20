#!/usr/bin/env tarantool
local fiber = require('fiber')

local test = require('tap').test('ttl delete nil')
local queue = require('queue')
local tnt = require('t.tnt')
tnt.cfg{}

-- gh-264: the ttl branch of the fiber iteration called :transform() on the
-- result of delete(), which is nil when the task has been deleted
-- concurrently between min() and delete(). That killed the ttl fiber.
local drivers = {'fifottl', 'utubettl'}
test:plan(#drivers)

for _, driver in ipairs(drivers) do
    test:test(driver, function(test)
        test:plan(4)
        local tube = queue.create_tube(driver .. '_nil', driver)
        local raw = tube.raw
        local ttl_fiber = raw.fiber

        -- Emulate a concurrent delete: the first delete() finds no task and
        -- returns nil, exactly as the driver's delete() does in that case.
        local calls = 0
        local delete = raw.delete
        raw.delete = function(self, id)
            calls = calls + 1
            if calls == 1 then
                self.space:delete(id)
                return nil
            end
            return delete(self, id)
        end

        tube:put('data', {ttl = 0.1, utube = 'utube'})
        fiber.sleep(0.3)
        test:is(calls, 1, 'delete() has returned nil to the ttl fiber')
        test:isnt(ttl_fiber:status(), 'dead', 'ttl fiber survives a nil from delete()')

        local task = tube:put('data', {ttl = 0.1, utube = 'utube'})
        fiber.sleep(0.3)
        test:is(calls, 2, 'delete() is called for the next expired task')
        test:isnil(raw.space:get(task[1]), 'the next expired task is deleted')

        raw.delete = nil
        tube:drop()
    end)
end

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
