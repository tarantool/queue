local log      = require('log')
local fiber    = require('fiber')

local state    = require('queue.abstract.state')

local util     = require('queue.util')
local qc       = require('queue.compat')
local num_type = qc.num_type
local str_type = qc.str_type

local tube = {}
local method = {}

local i_id              = 1
local i_status          = 2
local i_next_event      = 3
local i_ttl             = 4
local i_ttr             = 5
local i_pri             = 6
local i_created         = 7
local i_utube           = 8
local i_data            = 9

local function is_expired(task)
    local dead_event = task[i_created] + task[i_ttl]
    return (dead_event <= fiber.time64())
end

-- validate space of queue
local function validate_space(space)
    -- check indexes
    local indexes = {'task_id', 'status', 'utube', 'watch'}
    for _, index in pairs(indexes) do
        if space.index[index] == nil then
            error(string.format('space "%s" does not have "%s" index',
                space.name, index))
        end
    end
end

-- create space
function tube.create_space(space_name, opts)
    opts.ttl = opts.ttl or util.MAX_TIMEOUT
    opts.ttr = opts.ttr or opts.ttl
    opts.pri = opts.pri or 0

    local space_opts         = {}
    local if_not_exists      = opts.if_not_exists or false
    space_opts.temporary     = opts.temporary or false
    space_opts.engine        = opts.engine or 'memtx'
    space_opts.format = {
        {name = 'task_id', type = num_type()},
        {name = 'status', type = str_type()},
        {name = 'next_event', type = num_type()},
        {name = 'ttl', type = num_type()},
        {name = 'ttr', type = num_type()},
        {name = 'pri', type = num_type()},
        {name = 'created', type = num_type()},
        {name = 'utube', type = str_type()},
        {name = 'data', type = '*'}
    }

    -- 1        2       3           4    5    6    7,       8      9
    -- task_id, status, next_event, ttl, ttr, pri, created, utube, data
    local space = box.space[space_name]
    if if_not_exists and space then
        -- Validate the existing space.
        validate_space(box.space[space_name])
        return space
    end

    space = box.schema.create_space(space_name, space_opts)
    space:create_index('task_id', {
        type = 'tree',
        parts = {i_id, num_type()}
    })
    space:create_index('status', {
        type = 'tree',
        parts = {i_status, str_type(), i_pri, num_type(), i_id, num_type()}
    })
    space:create_index('watch', {
        type = 'tree',
        parts = {i_status, str_type(), i_next_event, num_type()},
        unique = false
    })
    space:create_index('utube', {
        type = 'tree',
        parts = {i_status, str_type(), i_utube, str_type(), i_id, num_type()}
    })
    return space
end

local delayed_state = { state.DELAYED }
local ttl_states    = { state.READY, state.BURIED }
local ttr_state     = { state.TAKEN }

local function utubettl_fiber_iteration(self, processed)
    local now       = util.time()
    local task      = nil
    local estimated = util.MAX_TIMEOUT

    -- delayed tasks
    task = self.space.index.watch:min(delayed_state)
    if task and task[i_status] == state.DELAYED then
        if now >= task[i_next_event] then
            task = self.space:update(task[i_id], {
                { '=', i_status, state.READY },
                { '=', i_next_event, task[i_created] + task[i_ttl] }
            })
            self:on_task_change(task, 'delayed')
            estimated = 0
            processed = processed + 1
        else
            estimated = tonumber(task[i_next_event] - now) / 1000000
        end
    end

    -- ttl tasks
    for _, state in pairs(ttl_states) do
        task = self.space.index.watch:min{ state }
        if task ~= nil and task[i_status] == state then
            if now >= task[i_next_event] then
                task = self:delete(task[i_id]):transform(2, 1, state.DONE)
                self:on_task_change(task, 'ttl')
                estimated = 0
                processed = processed + 1
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                estimated = et < estimated and et or estimated
            end
        end
    end

    -- ttr tasks
    task = self.space.index.watch:min(ttr_state)
    if task and task[i_status] == state.TAKEN then
        if now >= task[i_next_event] then
            task = self.space:update(task[i_id], {
                { '=', i_status, state.READY },
                { '=', i_next_event, task[i_created] + task[i_ttl] }
            })
            self:on_task_change(task, 'ttr')
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
        self.cond:wait(estimated)
    end

    return processed
end

-- watch fiber
local function utubettl_fiber(self)
    fiber.name('utubettl')
    log.info("Started queue utubettl fiber")
    local processed = 0

    while true do
        if box.info.ro == false then
            local stat, err = pcall(utubettl_fiber_iteration, self, processed)

            if not stat and not (err.code == box.error.READONLY) then
                log.error("error catched: %s", tostring(err))
                log.error("exiting fiber '%s'", fiber.name())
                return 1
            elseif stat then
                processed = err
            end
        else
            -- When switching the master to the replica, the fiber will be stopped.
            if self.sync_chan:get(0.1) ~= nil then
                log.info("Queue utubettl fiber was stopped")
                break
            end
        end
    end
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    validate_space(space)

    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space           = space,
        on_task_change  = function(self, task, stat_data)
            -- wakeup fiber
            if task ~= nil and self.fiber ~= nil then
                self.cond:signal(self.fiber:id())
            end
            on_task_change(task, stat_data)
        end,
        opts            = opts,
    }, { __index = method })

    self.cond  = qc.waiter()
    self.fiber = fiber.create(utubettl_fiber, self)
    self.sync_chan = fiber.channel()

    return self
end

-- cleanup internal fields in task
function method.normalize_task(self, task)
    return task and task:transform(i_next_event, i_data - i_next_event)
end

-- put task in space
function method.put(self, data, opts)
    local max

    -- Taking the maximum of the index is an implicit transactions, so it is
    -- always done with 'read-confirmed' mvcc isolation level.
    -- It can lead to errors when trying to make parallel 'put' calls with mvcc enabled.
    -- It is hapenning because 'max' for several puts in parallel will be the same since
    -- read confirmed isolation level makes visible all transactions that finished the commit.
    -- To fix it we wrap it with box.begin/commit and set right isolation level.
    -- Current fix does not resolve that bug in situations when we already are in transaction
    -- since it will open nested transactions.
    -- See https://github.com/tarantool/queue/issues/207
    -- See https://www.tarantool.io/ru/doc/latest/concepts/atomic/txn_mode_mvcc/

    if box.cfg.memtx_use_mvcc_engine and (not box.is_in_txn()) then
        box.begin({txn_isolation = 'read-committed'})
        max = self.space.index.task_id:max()
        box.commit()
    else
        max = self.space.index.task_id:max()
    end

    local id = max and max[i_id] + 1 or 0

    local status
    local ttl = opts.ttl or self.opts.ttl
    local ttr = opts.ttr or self.opts.ttr
    local pri = opts.pri or self.opts.pri or 0

    local next_event

    if opts.delay ~= nil and opts.delay > 0 then
        status = state.DELAYED
        ttl = ttl + opts.delay
        next_event = util.event_time(opts.delay)
    else
        status = state.READY
        next_event = util.event_time(ttl)
    end

    local task = self.space:insert{
        id,
        status,
        next_event,
        util.time(ttl),
        util.time(ttr),
        pri,
        util.time(),
        tostring(opts.utube),
        data
    }
    self:on_task_change(task, 'put')
    return task
end

local TIMEOUT_INFINITY_TIME = util.time(util.MAX_TIMEOUT)

-- touch task
function method.touch(self, id, delta)
    local ops = {
        {'+', i_next_event, delta},
        {'+', i_ttl,        delta},
        {'+', i_ttr,        delta}
    }
    if delta == util.MAX_TIMEOUT then
        ops = {
            {'=', i_next_event, delta},
            {'=', i_ttl,        delta},
            {'=', i_ttr,        delta}
        }
    end
    local task = self.space:update(id, ops)

    self:on_task_change(task, 'touch')
    return task
end

-- take task
function method.take(self)
    for s, t in self.space.index.status:pairs(state.READY, {iterator = 'GE'}) do
        if t[2] ~= state.READY then
            break
        elseif not is_expired(t) then
            local next_event = util.time() + t[i_ttr]
            local taken
            -- Taking the minimum is an implicit transactions, so it is
            -- always done with 'read-confirmed' mvcc isolation level.
            -- It can lead to errors when trying to make parallel 'take' calls with mvcc enabled.
            -- It is hapenning because 'min' for several takes in parallel will be the same since
            -- read confirmed isolation level makes visible all transactions that finished the commit.
            -- To fix it we wrap it with box.begin/commit and set right isolation level.
            -- Current fix does not resolve that bug in situations when we already are in transaction
            -- since it will open nested transactions.
            -- See https://github.com/tarantool/queue/issues/207
            -- See https://www.tarantool.io/ru/doc/latest/concepts/atomic/txn_mode_mvcc/
            if box.cfg.memtx_use_mvcc_engine and (not box.is_in_txn()) then
                box.begin({txn_isolation = 'read-committed'})
                taken = self.space.index.utube:min{state.TAKEN, t[i_utube]}
                box.commit()
            else
                taken = self.space.index.utube:min{state.TAKEN, t[i_utube]}
            end

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

local function process_neighbour(self, task, operation)
    self:on_task_change(task, operation)
    if task ~= nil then
        local neighbour = self.space.index.utube:select(
            {state.READY, task[i_utube]},
            {limit = 1}
        )[1]
        if neighbour ~= nil then
            self:on_task_change(neighbour)
        end
    end
    return task
end

-- delete task
function method.delete(self, id)
    local task = self.space:get(id)
    if task ~= nil then
        local is_taken = task[i_status] == state.TAKEN
        self.space:delete(id)
        task = task:transform(i_status, 1, state.DONE)
        if is_taken then
            return process_neighbour(self, task, 'delete')
        else
            self:on_task_change(task, 'delete')
            return task
        end
    end
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
            { '=', i_next_event, util.event_time(opts.delay) },
            { '+', i_ttl, util.time(opts.delay) }
        })
        if task ~= nil then
            return process_neighbour(self, task, 'release')
        end
    else
        task = self.space:update(id, {
            { '=', i_status, state.READY },
            { '=', i_next_event, util.time(task[i_created] + task[i_ttl]) }
        })
    end
    self:on_task_change(task, 'release')
    return task
end

-- bury task
function method.bury(self, id)
    -- The `i_next_event` should be updated because if the task has been
    -- "buried" after it was "taken" (and the task has "ttr") when the time in
    -- `i_next_event` will be interpreted as "ttl" in `utubettl_fiber_iteration`
    -- and the task will be deleted.
    local task = self.space:get{id}
    if task == nil then
        return
    end
    task = self.space:update(id, {
            { '=', i_status, state.BURIED },
            { '=', i_next_event, task[i_created] + task[i_ttl] }
    })

    return process_neighbour(
        self, task:transform(i_status, 1, state.BURIED), 'bury'
    )
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

-- get iterator to tasks in a certain state
function method.tasks_by_state(self, task_state)
    return self.space.index.status:pairs(task_state)
end

function method.truncate(self)
    self.space:truncate()
end

function method.start(self)
    if self.fiber then
        return
    end
    self.fiber = fiber.create(utubettl_fiber, self)
end

function method.stop(self)
    if not self.fiber then
        return
    end
    self.cond:signal(self.fiber:id())
    self.sync_chan:put(true)
    self.fiber = nil
end

return tube
