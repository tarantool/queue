local queue = nil

-- load all core drivers
local core_drivers = {
    fifo        = require('queue.abstract.driver.fifo'),
    fifottl     = require('queue.abstract.driver.fifottl'),
    utube       = require('queue.abstract.driver.utube'),
    utubettl    = require('queue.abstract.driver.utubettl'),
    limfifottl  = require('queue.abstract.driver.limfifottl')
}

local function register_driver(driver_name, tube_ctr)
    if type(tube_ctr.create_space) ~= 'function' or
        type(tube_ctr.new) ~= 'function' then
        error('tube control methods must contain functions "create_space"'
              .. ' and "new"')
    end
    queue.driver[driver_name] = tube_ctr
end

queue = setmetatable({
    driver = core_drivers,
    register_driver = register_driver,
}, { __index = function() print(debug.traceback()) error("Please run box.cfg{} first") end })

if rawget(box, 'space') == nil then
    local orig_cfg = box.cfg
    box.cfg = function(...)
        local result = { orig_cfg(...) }

        local abstract = require 'queue.abstract'
        for name, val in pairs(abstract) do
            rawset(queue, name, val)
        end
        abstract.driver = queue.driver
        setmetatable(queue, getmetatable(abstract))
        queue.start()

        return unpack(result)
    end
else
    queue = require 'queue.abstract'
    queue.register_driver = register_driver
    queue.driver = core_drivers
    queue.start()
end

return queue
