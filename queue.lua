local queue = { is_run = false }
local queue_methods = {}
local queue_mt = { __index = queue_methods }

local function init()
    setmetatable(queue, queue_mt)
    queue.is_run = true
    queue.schema = require 'queue.schema'
end

if rawget(box, 'space') == nil then
    local orig_cfg = box.cfg
    box.cfg = function(...)
        orig_cfg(...)
        init()
    end
    setmetatable(queue, {
        __index = function()
            box.error(box.error.PROC_LUA, "usage box.cfg{..} first")
        end
    })
else
    init()
end

return queue
