#!/usr/bin/env tarantool
local fiber = require('fiber')

local test = require('tap').test('ttl fiber errors')
local queue = require('queue')
local queue_state = require('queue.abstract.queue_state')
local tnt = require('t.tnt')
tnt.cfg{}

-- gh-263: an error inside a ttl iteration killed the ttl fiber for good,
-- and the next ro switch got the queue stuck in the ENDING state.
local drivers = {'fifottl', 'utubettl'}
test:plan(#drivers)

for _, driver in ipairs(drivers) do
    test:test(driver, function(test)
        test:plan(7)
        local fired = false
        local tube = queue.create_tube(driver .. '_errors', driver, {
            on_task_change = function(task, stat)
                if stat == 'ttl' and not fired then
                    fired = true
                    error('user callback failed once')
                end
            end,
        })
        local ttl_fiber = tube.raw.fiber

        tube:put('data', {ttl = 0.1, utube = 'utube'})
        fiber.sleep(0.3)
        test:ok(fired, 'the callback has raised an error on ttl')
        test:isnt(ttl_fiber:status(), 'dead', 'ttl fiber survives the error')

        local task = tube:put('data', {ttl = 0.1, utube = 'utube'})
        fiber.sleep(1.5) -- ttl plus the retry delay
        test:isnil(tube.raw.space:get(task[1]),
            'ttl processing continues after the error')

        box.cfg{read_only = true}
        test:ok(queue_state.poll(queue_state.states.WAITING, 10),
            'queue state changed to waiting')
        box.cfg{read_only = false}
        test:ok(queue_state.poll(queue_state.states.RUNNING, 10),
            'queue state changed to running')
        test:isnt(tube.raw.fiber, nil, 'ttl fiber is registered')
        test:isnt(tube.raw.fiber:status(), 'dead', 'ttl fiber is running')

        tube:drop()
    end)
end

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
