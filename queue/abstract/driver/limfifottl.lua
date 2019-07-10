local qc       = require('queue.compat')

local fiber    = require('fiber')
local fifottl  = require('queue.abstract.driver.fifottl')

local tube = {}
local methods = {}

tube.create_space = fifottl.create_space

-- start tube on space
function tube.new(space, on_task_change, opts)
    return setmetatable(
        {
            capacity = opts.capacity or 0,
            parent = fifottl.new(space, on_task_change, opts)
        },
        {
            __index = function(self, key)
                if methods[key] ~= nil then
                    return methods[key]
                else
                    return self.parent[key]
                end
            end
        })
end

-- put task in space
function methods.put(self, data, opts)
    local timeout = opts.timeout or 0
    if opts.delay ~= nil and opts.delay > 0 and timeout > 0 then
        timeout = timeout + opts.delay
    end
    timeout = timeout * 1000000
    
    while true do
        local tube_size = self.space:count()
        if tube_size < self.capacity or self.capacity == 0 then
            return self.parent.put(self, data, opts)
        else
            if timeout == 0 then
                return nil
            end

            local started = fiber.time64()

            local waiter = qc.waiter()
            waiter:wait(0.1)
            waiter:free()

            local elapsed = fiber.time64() - started
            timeout = timeout > elapsed and timeout - elapsed or 0
        end
    end
end

return tube
