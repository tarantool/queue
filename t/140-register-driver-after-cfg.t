#!/usr/bin/env tarantool
local tap = require('tap')
local tnt = require('t.tnt')

local test = tap.test('test driver register')
test:plan(2)

local mock_tube = {
    create_space = function() end,
    new = function() end
}

-- As opposed to 001-tube-init.t, queue initialization
-- and driver registration are done after cfg().
local function check_driver_register()
    tnt.cfg()
    local queue = require('queue')
    queue.register_driver('mock', mock_tube)
    test:is(queue.driver.mock, mock_tube, 'driver has been registered')

    local standart_drivers = {
        'fifo',
        'fifottl',
        'limfifottl',
        'utube',
        'utubettl'
    }
    local check_standart_drivers = true

    for _, v in pairs(standart_drivers) do
        if queue.driver[v] == nil then
            check_standart_drivers = false
            break
        end
    end

    test:ok(check_standart_drivers, 'standard drivers are defined')
end

check_driver_register()

tnt.finish()
os.exit(test:check() and 0 or 1)
