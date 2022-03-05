local queue_state = require('queue.abstract.queue_state')
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
    if queue.driver[driver_name] then
        error(('overriding registered driver "%s"'):format(driver_name))
    end
    queue.driver[driver_name] = tube_ctr
end

local deferred_opts = {}

-- We cannot call queue.cfg() while tarantool is in read_only mode.
-- This method stores settings for later original queue.cfg() call.
local function deferred_cfg(opts)
    opts = opts or {}

    for k, v in pairs(opts) do
        deferred_opts[k] = v
    end
end

queue = setmetatable({
    driver = core_drivers,
    register_driver = register_driver,
    state = queue_state.show,
    cfg = deferred_cfg,
}, { __index = function()
        print(debug.traceback())
        error('Please configure box.cfg{} in read/write mode first')
    end
})

-- Used to store the original methods
local orig_cfg = nil
local orig_call = nil

local wrapper_impl

local function cfg_wrapper(...)
    box.cfg = orig_cfg
    return wrapper_impl(...)
end

local function cfg_call_wrapper(cfg, ...)
    local cfg_mt = getmetatable(box.cfg)
    cfg_mt.__call = orig_call
    return wrapper_impl(...)
end

local function wrap_box_cfg()
    if type(box.cfg) == 'function' then
        -- box.cfg before the first box.cfg call
        orig_cfg = box.cfg
        box.cfg = cfg_wrapper
    elseif type(box.cfg) == 'table' then
        -- box.cfg after the first box.cfg call
        local cfg_mt = getmetatable(box.cfg)
        orig_call = cfg_mt.__call
        cfg_mt.__call = cfg_call_wrapper
    else
        error('The box.cfg type is unexpected: ' .. type(box.cfg))
    end
end

function wrapper_impl(...)
    local result = { pcall(box.cfg,...) }
    if result[1] then
        table.remove(result, 1)
    else
        wrap_box_cfg()
        error(result[2])
    end

    if box.info.ro == false then
        local abstract = require 'queue.abstract'
        for name, val in pairs(abstract) do
            rawset(queue, name, val)
        end
        abstract.driver = queue.driver
        -- Now the "register_driver" method from abstract will be used.
        queue.register_driver = nil
        setmetatable(queue, getmetatable(abstract))
        queue.start()
        queue.cfg(deferred_opts)
    else
        -- Delay a start until the box will be configured
        -- with read_only = false
        wrap_box_cfg()
    end
    return unpack(result)
end

--- Implementation of the “lazy start” procedure.
-- The queue module is loaded immediately if the instance was
-- configured with read_only = false. Otherwise, a start is
-- delayed until the instance will be configured with read_only = false.
local function queue_init()
    if rawget(box, 'space') ~= nil and box.info.ro == false then
        -- The box was configured with read_only = false
        queue = require('queue.abstract')
        queue.driver = core_drivers
        queue.start()
    else
        wrap_box_cfg()
    end
end

queue_init()

return queue
