local function index_unconfigured()
    box.error(box.error.PROC_LUA, "Please run box.cfg{} first")
end

local queue = {}
setmetatable(queue, { __index = index_unconfigured })


if rawget(box, 'space') == nil then
    local orig_cfg = box.cfg
    box.cfg = function(...)
        local result = { orig_cfg(...) }

        local abstract = require 'queue.abstract'
        for k, v in pairs(abstract) do
            rawset(queue, k, v)
        end
        setmetatable(queue, getmetatable(abstract))
        queue.start()

        return unpack(result)
    end
else
    queue = require 'queue.abstract'
end

return queue
