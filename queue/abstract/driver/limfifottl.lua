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

    return setmetatable({
        put = put,
        len = len
    }, {__index = state.parent})
end

return tube
