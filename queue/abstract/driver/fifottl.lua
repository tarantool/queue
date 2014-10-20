local state = require 'queue.abstract.state'
local tube = {}
local method = {}
local log = require 'log'
local json = require 'json'
local fiber = require 'fiber'

local TIMEOUT_INFINITY  = 365 * 86400 * 500


local i_id              = 1
local i_status          = 2
local i_next_event      = 3
local i_ttl             = 4
local i_ttr             = 5
local i_pri             = 6
local i_created         = 7
local i_data            = 8


local function time(tm)
    if tm == nil then
        tm = fiber.time()
    end
    return tonumber64(tm * 1000000)
end

local function event_time(timeout)
    if timeout == nil then
        box.error(box.error.PROC_LUA, debug.traceback())
    end
    return tonumber64((fiber.time() + timeout) * 1000000)
end

-- create space
function tube.create_space(space_name, opts)
    if opts.ttl == nil then
        opts.ttl = TIMEOUT_INFINITY
    end

    if opts.ttr == nil then
        opts.ttr = opts.ttl
    end

    if opts.pri == nil then
        opts.pri = 0
    end

    local space_opts = {}
    space_opts.temporary = opts.temporary

    -- 1        2       3           4    5    6    7,       8
    -- task_id, status, next_event, ttl, ttr, pri, created, data
    local space = box.schema.create_space(space_name, space_opts)

    space:create_index('task_id', { type = 'tree', parts = { 1, 'num' }})
    space:create_index('status',
        { type = 'tree', parts = { 2, 'str', 6, 'num', 1, 'num' } })
    space:create_index('watch',
        { type = 'tree', parts = { 2, 'str', 3, 'num' }})
    return space
end


-- start tube on space
function tube.new(space, on_task_change, opts)
    if on_task_change == nil then
        on_task_change = function() end
    end
    local self = {
        space           = space,
        on_task_change  = function(self, task)
            -- wakeup fiber
            if task ~= nil and task[i_status] == state.DELAYED then
                if self.fiber ~= nil then
                    self.fiber:wakeup()
                end
            end
            on_task_change(task)
        end,
        opts            = opts,
    }
    setmetatable(self, { __index = method })

    self.fiber = fiber.create(self._fiber, self)

    return self
end

-- watch fiber
function method._fiber(self)
    fiber.name('fifottl')
    while true do
        local delayed = self.space.index.watch:min{ state.DELAYED }
        if delayed and delayed[i_status] == state.DELAYED then
            local now = time()
            if now >= delayed[i_next_event] then
                delayed = self.space:update(delayed[i_id], {
                    { '=', i_status, state.READY },
                    { '=', i_next_event, delayed[i_created] + delayed[i_ttl] }
                })
                self:on_task_change(delayed)

            else
                local estimated =
                    tonumber(delayed[i_next_event] - now) / 1000000
                fiber.sleep(estimated)
            end
        else
            -- there is no delayed tasks
            fiber.sleep(TIMEOUT_INFINITY)
        end
    end
end


-- cleanup internal fields in task
function method.normalize_task(self, task)
    if task ~= nil then
        return task:transform(3, 5)
    end
end

-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = 0
    if max ~= nil then
        id = max[i_id] + 1
    end

    local status
    local ttl = opts.ttl or self.opts.ttl
    local ttr = opts.ttr or self.opts.ttr
    local pri = opts.pri or self.opts.pri or 0
    
    local next_event

    if opts.delay ~= nil and opts.delay > 0 then
        status = state.DELAYED
        ttl = ttl + opts.delay
        next_event = event_time(opts.delay)
    else
        status = state.READY
        next_event = event_time(ttl)
    end



    local task = self.space:insert{
            id,
            status,
            next_event,
            time(ttl),
            time(ttr),
            pri,
            time(),
            data
    }
    self:on_task_change(task)
    return task
end


-- take task
function method.take(self)
    local task = self.space.index.status:min{state.READY}
    if task == nil or task[i_status] ~= state.READY then
        return
    end

    local next_event = event_time(self.opts.ttr)
    local dead_event = task[i_created] + time(self.opts.ttl)
    if next_event > dead_event then
        next_event = dead_event
    end

    task = self.space:update(task[i_id], {
        { '=', i_status, state.TAKEN },
        { '=', i_next_event, next_event  }
    })
    return task
end

-- delete task
function method.delete(self, id)
    return self.space:delete(id)
end

-- release task
function method.release(self, id, opts)
    local task = self.space:get{id}
    if task == nil then
        return
    end
    if opts.delay ~= nil and opts.delay > 0 then
        task = self.space:update(id, {
            { '=', i_status, state.DELAYED },
            { '=', i_next_event, event_time(opts.delay) },
            { '+', i_ttl, opts.delay }
        })
    else
        task = self.space:update(id, {
            { '=', i_status, state.READY },
            { '=', i_next_event, task[i_created] + task[i_ttl] }
        })
    end
    self:on_task_change(task)
    return task
end

-- bury task
function method.bury(self, id)
    local task = self.space:update(id, {{ '=', i_status, state.BURIED }})
    if task == nil then
        self:on_task_change()
    end
    return task
end

-- unbury several tasks
function method.kick(self, count)
    for i = 1, count do
        local task = self.space.index.status:min{ state.BURIED }
        if task == nil then
            return i - 1
        end
        if task[i_status] ~= state.BURIED then
            return i - 1
        end

        task = self.space:update(task[i_id], {{ '=', i_status, state.READY }})
        self:on_task_change(task)
    end
    return count
end

-- peek task
function method.peek(self, id)
    return self.space:get{id}
end


return tube
