local log      = require('log')
local fiber    = require('fiber')

local state    = require('queue.abstract.state')
local num_type = require('queue.compat').num_type
local str_type = require('queue.compat').str_type

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
local i_utube           = 8
local i_data            = 9

local function time(tm)
    tm = tm and tm * 1000000 or fiber.time64()
    return 0ULL + tm
end

local function event_time(timeout)
    if timeout == nil then
        error(debug.traceback())
    end
    return fiber.time64() + time(timeout)
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

    local space_opts         = {}
    local if_not_exists      = opts.if_not_exists or false
    space_opts.temporary     = opts.temporary or false
    space_opts.if_not_exists = if_not_exists

    -- 1        2       3           4    5    6    7,       8
    -- task_id, status, next_event, ttl, ttr, pri, created, data
    local space = box.schema.create_space(space_name, space_opts)

    space:create_index('task_id', {
        type = 'tree',
        parts = {i_id, num_type()},
        if_not_exists = if_not_exists
    })
    space:create_index('status', {
        type = 'tree',
        parts = {i_status, str_type(), i_pri, num_type(), i_id, num_type()},
        if_not_exists = if_not_exists
    })
    space:create_index('watch', {
        type = 'tree',
        parts = {i_status, str_type(), i_next_event, num_type()},
        unique = false,
        if_not_exists = if_not_exists
    })
    space:create_index('utube', {
        type = 'tree',
        parts = {i_status, str_type(), i_utube, str_type(), i_id, num_type()},
        if_not_exists = if_not_exists
    })
    return space
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space           = space,
        on_task_change  = function(self, task, stat_data)
            -- wakeup fiber
            if task ~= nil then
                if self.fiber ~= nil then
                    if self.fiber:id() ~= fiber.id() then
                        self.fiber:wakeup()
                    end
                end
            end
            on_task_change(task, stat_data)
        end,
        opts            = opts,
    }, { __index = method})

    self.fiber = fiber.create(self._fiber, self)

    return self
end

local function process_neighbour(self, task, operation)
    self:on_task_change(task, operation)
    if task ~= nil then
        local neighbour = self.space.index.utube:min{state.READY, task[i_utube]}
        if neighbour ~= nil and neighbour[i_status] == state.READY then
            self:on_task_change(neighbour)
        end
    end
    return task
end

-- watch fiber
function method._fiber(self)
    fiber.name('fifottl')
    log.info("Started queue utubettl fiber")
    local estimated
    local ttl_statuses = { state.READY, state.BURIED }
    local now, task
    local processed = 0

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
                processed = processed + 1
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
                    processed = processed + 1
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
                processed = processed + 1
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                estimated = et < estimated and et or estimated
            end
        end

        if estimated > 0 or processed > 1000 then
            -- free refcounter
            estimated = processed > 1000 and 0 or estimated
            estimated = estimated > 0 and estimated or 0
            processed = 0
            task = nil
            fiber.sleep(estimated)
        end
    end
end

-- cleanup internal fields in task
function method.normalize_task(self, task)
    return task and task:transform(i_next_event, i_data - i_next_event)
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
        tostring(opts.utube),
        data
    }
    self:on_task_change(task, 'put')
    return task
end

local TIMEOUT_INFINITY_TIME = time(TIMEOUT_INFINITY)

-- touch task
function method.touch(self, id, increment_seconds)
    if increment_seconds < 0 then
        error("Increment can't be less than zero")
    elseif increment_seconds > TIMEOUT_INFINITY then
        increment_seconds = TIMEOUT_INFINITY
    end

    local task = self:peek{id}
    if increment_seconds == 0 or task[i_ttr] >= TIMEOUT_INFINITY_TIME then
        return task
    end

    local increment = 0ULL
    if increment_seconds == nil then
        increment = task[i_ttr] or task[i_ttr]
    else
        increment = time(increment_seconds)
    end

    task = self.space:update{
        id,
        {{i_ttl, '+', increment}},
        {{i_ttr, '+', increment}}
    }

    self:on_task_change(task, 'touch')
    return task
end

-- take task
function method.take(self)
    for s, t in self.space.index.status:pairs(state.READY, {iterator = 'GE'}) do
        if t[2] ~= state.READY then
            break
        elseif not is_expired(t) then
            local next_event = time() + t[i_ttr]
            local taken = self.space.index.utube:min{state.TAKEN, t[i_utube]}
            if taken == nil or taken[i_status] ~= state.TAKEN then
                t = self.space:update(t[1], {
                    { '=', i_status, state.TAKEN },
                    { '=', i_next_event, next_event }
                })
                self:on_task_change(t, 'take')
                return t
            end
        end
    end
end

-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task ~= nil then
        task = task:transform(i_status, 1, state.DONE)
        return process_neighbour(self, task, 'delete')
    end
    self:on_task_change(task, 'delete')
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
        if task ~= nil then
            return process_neighbour(self, task, 'release')
        end
    else
        task = self.space:update(id, {
            { '=', i_status, state.READY },
            { '=', i_next_event, time(task[i_created] + task[i_ttl]) }
        })
    end
    self:on_task_change(task, 'release')
    return task
end

-- bury task
function method.bury(self, id)
    local task = self.space:update(id, {{ '=', i_status, state.BURIED }})
    if task ~= nil then
        return process_neighbour(
            self, task:transform(i_status, 1, state.BURIED), 'bury'
        )
    end
    self:on_task_change(task, 'bury')
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

function method.truncate(self)
    self.space:truncate()
end

return tube
