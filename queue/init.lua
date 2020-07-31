local queue = nil

local function register_driver(driver_name, tube_ctr)
    if type(tube_ctr.create_space) ~= 'function' or
        type(tube_ctr.new) ~= 'function' then
        error('tube control methods must contain functions "create_space"'
              .. ' and "new"')
    end
    queue.driver[driver_name] = tube_ctr
end

queue = setmetatable({
    driver = {},
    register_driver = register_driver,
}, { __index = function() print(debug.traceback()) error("Please run box.cfg{} first") end })

if rawget(box, 'space') == nil then
    local orig_cfg = box.cfg
    box.cfg = function(...)
        local result = { orig_cfg(...) }

        local abstract = require 'queue.abstract'
        for name, val in pairs(abstract) do
            if name == 'driver' then
                for driver_name, driver in pairs(queue.driver) do
                    if abstract.driver[driver_name] then
                        error(('overriding default driver "%s"'):format(driver_name))
                    end
                    abstract.driver[driver_name] = driver
                end
            end
            rawset(queue, name, val)
        end
        setmetatable(queue, getmetatable(abstract))
        queue.start()

        return unpack(result)
    end
else
    queue = require 'queue.abstract'
    queue.register_driver = register_driver
    queue.start()
end

return queue
