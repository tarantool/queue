local fiber    = require('fiber')
local fifottl  = require('queue.abstract.driver.fifottl')

local tube = {}

tube.create_space = function(space_name, opts)
    if opts.engine == 'vinyl' then
        error('limfifottl queue does not support vinyl engine')
    end
    return fifottl.create_space(space_name, opts)
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    local state = {
        capacity = opts.capacity or 0,
        parent = fifottl.new(space, on_task_change, opts)
    }

    -- put task in space
    local put = function (self, data, opts)
        local timeout = opts.timeout or 0
        local started = tonumber(fiber.time())

        while true do
            local tube_size = self.space:len()
            if tube_size < state.capacity or state.capacity == 0 then
                return state.parent.put(self, data, opts)
            else
                if tonumber(fiber.time()) - started > timeout then
                    return nil
                end
                fiber.sleep(.01)
            end
        end
    end

    local len = function (self)
        return self.space:len()
    end

    -- The ttl fiber is registered in the parent object: forward the
    -- lifecycle methods there so that stop() does not shadow the
    -- registration in this wrapper.
    local start = function (self)
        return state.parent:start()
    end

    local stop = function (self)
        return state.parent:stop()
    end

    local drop = function (self)
        return state.parent:drop()
    end

    return setmetatable({
        put = put,
        len = len,
        start = start,
        stop = stop,
        drop = drop,
    }, {__index = state.parent})
end

return tube
