local log   = require 'log'
local fiber = require 'fiber'
local state = require 'queue.abstract.state'

local tube = {}
local method = {}

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
    tm = tm and tm * 1000000 or fiber.time64()
    return 0ULL + tm
end

local function event_time(timeout)
    if timeout == nil then
        box.error(box.error.PROC_LUA, debug.traceback())
    end
    return 0ULL + ((fiber.time() + timeout) * 1000000)
end

local function is_expired(task)
    local dead_event = task[i_created] + task[i_ttl]
    return (dead_event <= fiber.time64())
end

-- create space
function tube.create_space(space_name, opts)
    opts.ttl = opts.ttl or TIMEOUT_INFINITY
    opts.ttr = opts.ttr or opts.ttl
    opts.pri = opts.pri or 0

    local space_opts = {}
    space_opts.temporary = opts.temporary

    -- 1        2       3           4    5    6    7,       8
    -- task_id, status, next_event, ttl, ttr, pri, created, data
    local space = box.schema.create_space(space_name, space_opts)

    space:create_index('task_id', { type = 'tree', parts = { i_id, 'num' }})
    space:create_index('status',
        { type = 'tree',
            parts = { i_status, 'str', i_pri, 'num', i_id, 'num' }})
    space:create_index('watch',
        { type = 'tree', parts = { i_status, 'str', i_next_event, 'num' },
            unique = false})
    return space
end


-- start tube on space
function tube.new(space, on_task_change, opts)
    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space           = space,
        on_task_change  = function(self, task, stats_data)
            -- wakeup fiber
            if task ~= nil then
                if self.fiber ~= nil then
                    if self.fiber:id() ~= fiber.id() then
                        self.fiber:wakeup()
                    end
                end
            end
            on_task_change(task, stats_data)
        end,
        opts            = opts,
    }, { __index = method })

    self.fiber = fiber.create(self._fiber, self)

    return self
end

-- watch fiber
function method._fiber(self)
    fiber.name('fifottl')
    log.info("Started queue fifottl fiber")
    local estimated
    local ttl_statuses = { state.READY, state.BURIED }
    local now, task

    while true do
        estimated = TIMEOUT_INFINITY
        now = time()

        -- delayed tasks
        task = self.space.index.watch:min{ state.DELAYED }
        if task and task[i_status] == state.DELAYED then
            if now >= task[i_next_event] then
                task = self.space:update(task[i_id], {
                    { '=', i_status, state.READY },
                    { '=', i_next_event, task[i_created] + task[i_ttl] }
                })
                self:on_task_change(task)
                estimated = 0
            else
                estimated = tonumber(task[i_next_event] - now) / 1000000
            end
        end

        -- ttl tasks
        for _, state in pairs(ttl_statuses) do
            task = self.space.index.watch:min{ state }
            if task ~= nil and task[i_status] == state then
                if now >= task[i_next_event] then
                    self.space:delete(task[i_id])
                    self:on_task_change(task:transform(2, 1, state.DONE))
                    estimated = 0
                else
                    local et = tonumber(task[i_next_event] - now) / 1000000
                    estimated = et < estimated and et or estimated
                end
            end
        end

        -- ttr tasks
        task = self.space.index.watch:min{ state.TAKEN }
        if task and task[i_status] == state.TAKEN then
            if now >= task[i_next_event] then
                task = self.space:update(task[i_id], {
                    { '=', i_status, state.READY },
                    { '=', i_next_event, task[i_created] + task[i_ttl] }
                })
                self:on_task_change(task)
                estimated = 0
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                estimated = et < estimated and et or estimated
            end
        end


        if estimated > 0 then
            -- free refcounter
            task = nil
            fiber.sleep(estimated)
        end
    end
end


-- cleanup internal fields in task
function method.normalize_task(self, task)
    return task and task:transform(3, 5)
end

-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = max and max[i_id] + 1 or 0

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
    self:on_task_change(task, 'put')
    return task
end


-- take task
function method.take(self)
    local offset = 0
    local task = nil
    while true do
        task = self.space.index.status:select({state.READY},
                {offset=offset, limit=1, iterator='GE'})[1]
        if task == nil or task[i_status] ~= state.READY then
            return
        elseif is_expired(task) then
            offset = offset + 1
        else
            break;
        end
    end

    local next_event = time() + task[i_ttr]
    local dead_event = task[i_created] + task[i_ttl]
    if next_event > dead_event then
        next_event = dead_event
    end

    task = self.space:update(task[i_id], {
        { '=', i_status, state.TAKEN },
        { '=', i_next_event, next_event  }
    })
    self:on_task_change(task, 'take')
    return task
end

-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task ~= nil then
        task = task:transform(2, 1, state.DONE)
    end
    self:on_task_change(task, 'delete')
    return task
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
    self:on_task_change(task, 'release')
    return task
end

-- bury task
function method.bury(self, id)
    local task = self.space:update(id, {{ '=', i_status, state.BURIED }})
    self:on_task_change(task, 'bury')
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
        self:on_task_change(task, 'kick')
    end
    return count
end

-- peek task
function method.peek(self, id)
    return self.space:get{id}
end

return tube
