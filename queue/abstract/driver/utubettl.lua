local log      = require('log')
local fiber    = require('fiber')

local state    = require('queue.abstract.state')

local util     = require('queue.util')
local qc       = require('queue.compat')
local num_type = qc.num_type
local str_type = qc.str_type

local tube = {}
local method = {}

tube.STORAGE_MODE_DEFAULT = "default"
tube.STORAGE_MODE_READY_BUFFER = "ready_buffer"

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
    local indexes = {'task_id', 'status', 'utube', 'watch', 'utube_pri'}
    for _, index in pairs(indexes) do
        if space.index[index] == nil then
            error(string.format('space "%s" does not have "%s" index',
                space.name, index))
        end
    end
end

-- validate ready buffer space of queue
local function validate_space_ready_buffer(space)
    -- check indexes
    local indexes = {'task_id', 'utube', 'pri'}
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
    space:create_index('utube_pri', {
        type = 'tree',
        parts = {i_status, str_type(), i_utube, str_type(), i_pri, num_type(), i_id, num_type()}
    })
    return space
end

local delayed_state = { state.DELAYED }
local ttl_states    = { state.READY, state.BURIED }
local ttr_state     = { state.TAKEN }

-- Find the first ready task for given 'utube'.
-- Utube is also checked for the absence of 'TAKEN' tasks.
local function put_next_ready(self, utube)
    local taken = self.space.index.utube:min{state.TAKEN, utube}
    if taken == nil or taken[i_status] ~= state.TAKEN then
        local next_task = self.space.index.utube_pri:min{state.READY, utube}
        if next_task == nil or next_task[i_status] ~= state.READY then
            return
        end
        -- Ignoring ER_TUPLE_FOUND error, if a tuple with the same task_id
        -- or utube name is already in the space.
        -- Note that both task_id and utube indexes are unique, so there will be
        -- no duplicates: each task_id can occur in the space not more than once,
        -- there can be no more than one task from each utube in a space.
        pcall(self.space_ready_buffer.insert, self.space_ready_buffer, {next_task[i_id], utube, next_task[i_pri]})
    end
end

-- Check if given task has lowest priority in the ready_buffer for the given 'utube'.
-- If so, current task for the given 'utube' is replaced in the ready_buffer.
-- Utube is also checked for the absence of 'TAKEN' tasks.
local function put_ready(self, id, utube, pri)
    local taken = self.space.index.utube:min{state.TAKEN, utube}
    if taken == nil or taken[i_status] ~= state.TAKEN then
        local current_task = self.space.index.utube_pri:min{state.READY, utube}
        if current_task[i_status] ~= state.READY or
                current_task[i_pri] < pri or (current_task[i_pri] == pri and current_task[i_id] < id) then
            return
        end
        if current_task[i_pri] > pri then
            self.space_ready_buffer:delete(current_task[id])
        end
        -- Ignoring ER_TUPLE_FOUND error, if a tuple with the same task_id
        -- or utube name is already in the space.
        -- Note that both task_id and utube indexes are unique, so there will be
        -- no duplicates: each task_id can occur in the space not more than once,
        -- there can be no more than one task from each utube in a space.
        pcall(self.space_ready_buffer.insert, self.space_ready_buffer, {id, utube, pri})
    end
end

-- Delete task from the ready_buffer and find next ready task from the same 'utube' to replace it.
local function delete_ready(self, id, utube)
    self.space_ready_buffer:delete(id)
    put_next_ready(self, utube)
end

-- Try to update the current task in ready_buffer for the given 'utube'.
local function update_ready(self, id, utube, pri)
    local prev_task = self.space_ready_buffer.index.utube:get{utube}
    if prev_task ~= nil then
        if prev_task[3] > pri or (prev_task[3] == pri and prev_task[1] > id) then
            self.space_ready_buffer:delete(prev_task[1])
            self.space_ready_buffer:insert({id, utube, pri})
        end
    else
        put_ready(self, id, utube, pri)
    end
end

local function commit()
    box.commit()
end

local function empty()
end

-- Start transaction with the correct options, if the transaction is not already running
-- and current engine is not 'vinyl'.
local function begin_if_not_in_txn(self)
    local transaction_opts = {}
    if box.cfg.memtx_use_mvcc_engine then
        transaction_opts = {txn_isolation = 'read-committed'}
    end

    -- Implemented only for memtx engine for now.
    -- https://github.com/tarantool/queue/issues/230.
    if not box.is_in_txn() and self.opts.engine ~= 'vinyl' then
        box.begin(transaction_opts)
        return commit
    else
        return empty
    end
end

local function utubettl_fiber_iteration(self, processed)
    local now       = util.time()
    local task      = nil
    local estimated = util.MAX_TIMEOUT

    local commit_func = begin_if_not_in_txn(self)
    local commited = false

    -- delayed tasks
    task = self.space.index.watch:min(delayed_state)
    if task and task[i_status] == state.DELAYED then
        if now >= task[i_next_event] then
            task = self.space:update(task[i_id], {
                { '=', i_status, state.READY },
                { '=', i_next_event, task[i_created] + task[i_ttl] }
            })

            if self.ready_space_mode then
                update_ready(self, task[i_id], task[i_utube], task[i_pri])
            end
            commit_func()
            commited = true

            self:on_task_change(task, 'delayed')
            estimated = 0
            processed = processed + 1
        else
            estimated = tonumber(task[i_next_event] - now) / 1000000
        end
    end
    if not commited then
        commit_func()
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

    commit_func = begin_if_not_in_txn(self)
    commited = false
    -- ttr tasks
    task = self.space.index.watch:min(ttr_state)
    if task and task[i_status] == state.TAKEN then
        if now >= task[i_next_event] then
            task = self.space:update(task[i_id], {
                { '=', i_status, state.READY },
                { '=', i_next_event, task[i_created] + task[i_ttl] }
            })

            if self.ready_space_mode then
                put_ready(self, task[i_id], task[i_utube], task[i_pri])
            end
            commit_func()
            commited = true

            self:on_task_change(task, 'ttr')
            estimated = 0
            processed = processed + 1
        else
            local et = tonumber(task[i_next_event] - now) / 1000000
            estimated = et < estimated and et or estimated
        end
    end
    if not commited then
        commit_func()
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
        end

        if self.sync_chan:get(0.1) ~= nil then
            log.info("Queue utubettl fiber was stopped")
            break
        end
    end
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    validate_space(space)

    local space_ready_buffer_name = space.name .. "_ready_buffer"
    local space_ready_buffer = box.space[space_ready_buffer_name]
    -- Feature implemented only for memtx engine for now.
    -- https://github.com/tarantool/queue/issues/230.
    if opts.storage_mode == tube.STORAGE_MODE_READY_BUFFER and opts.engine == 'vinyl' then
        error(string.format('"%s" storage mode cannot be used with vinyl engine',
                tube.STORAGE_MODE_READY_BUFFER))
    end

    local ready_space_mode = (opts.storage_mode == tube.STORAGE_MODE_READY_BUFFER or false)
    if ready_space_mode then
        if space_ready_buffer == nil then
            local space_opts     = {}
            space_opts.temporary = opts.temporary or false
            space_opts.engine    = opts.engine or 'memtx'
            space_opts.format = {
                {name = 'task_id', type = num_type()},
                {name = 'utube', type = str_type()},
                {name = 'pri', type = num_type()},
            }

            -- Create a space for first ready tasks from each utube.
            space_ready_buffer = box.schema.create_space(space_ready_buffer_name, space_opts)
            space_ready_buffer:create_index('task_id', {
                type = 'tree',
                parts = {1, num_type()},
                unique = true,
            })
            space_ready_buffer:create_index('utube', {
                type = 'tree',
                parts = {2, str_type()},
                unique = true,
            })
            space_ready_buffer:create_index('pri', {
                type = 'tree',
                parts = {3, num_type(), 1, num_type()},
            })
        else
            validate_space_ready_buffer(space_ready_buffer)
            if space:len() == 0 then
                space_ready_buffer:truncate()
            end
        end
    end

    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space              = space,
        space_ready_buffer = space_ready_buffer,
        on_task_change     = function(self, task, stat_data)
            -- wakeup fiber
            if task ~= nil and self.fiber ~= nil then
                self.cond:signal(self.fiber:id())
            end
            on_task_change(task, stat_data)
        end,
        opts             = opts,
        ready_space_mode = ready_space_mode,
    }, { __index = method })

    self.cond  = qc.waiter()
    self.fiber = fiber.create(utubettl_fiber, self)
    self.sync_chan = fiber.channel(1)

    return self
end

-- cleanup internal fields in task
function method.normalize_task(self, task)
    return task and task:transform(i_next_event, i_data - i_next_event)
end

-- put task in space
function method.put(self, data, opts)
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
    local commit_func = begin_if_not_in_txn(self)

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
    if self.ready_space_mode and status == state.READY then
        put_ready(self, task[i_id], task[i_utube], task[i_pri])
    end

    commit_func()

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

-- Take the first task form the ready_buffer.
local function take_ready(self)
    while true do
        local commit_func = begin_if_not_in_txn(self)

        local task_ready = self.space_ready_buffer.index.pri:min()
        if task_ready == nil then
            commit_func()
            return nil
        end

        local id = task_ready[1]
        local task = self.space:get(id)
        local take_complete = false
        local take_ttl = false

        if task[i_status] == state.READY then
            if not is_expired(task) then
                local next_event = util.time() + task[i_ttr]

                local taken = self.space.index.utube:min{state.TAKEN, task[i_utube]}

                if taken == nil or taken[i_status] ~= state.TAKEN then
                    task = self.space:update(task[1], {
                        { '=', i_status, state.TAKEN },
                        { '=', i_next_event, next_event }
                    })

                    self.space_ready_buffer:delete(task[i_id])
                    take_complete = true
                end
            else
                task = self:delete(task[i_id]):transform(2, 1, state.DONE)
                take_ttl = true
            end
        end

        commit_func()

        if take_complete then
            self:on_task_change(task, 'take')
            return task
        elseif take_ttl then
            self:on_task_change(task, 'ttl')
        end
    end
end

local function take(self)
    for s, t in self.space.index.status:pairs(state.READY, {iterator = 'GE'}) do
        if t[2] ~= state.READY then
            break
        elseif not is_expired(t) then
            local next_event = util.time() + t[i_ttr]
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
            local commit_func = begin_if_not_in_txn(self)
            local taken = self.space.index.utube:min{state.TAKEN, t[i_utube]}
            local take_complete = false

            if taken == nil or taken[i_status] ~= state.TAKEN then
                t = self.space:update(t[1], {
                    { '=', i_status, state.TAKEN },
                    { '=', i_next_event, next_event }
                })
                take_complete = true
            end

            commit_func()
            if take_complete then
                self:on_task_change(t, 'take')
                return t
            end
        end
    end
end

-- take task
function method.take(self)
    if self.ready_space_mode then
        return take_ready(self)
    end
    return take(self)
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
    local commit_func = begin_if_not_in_txn(self)

    local task = self.space:get(id)
    if task ~= nil then
        local is_taken = task[i_status] == state.TAKEN
        self.space:delete(id)

        if self.ready_space_mode then
            if task[i_status] == state.TAKEN then
                put_next_ready(self, task[i_utube])
            elseif task[i_status] == state.READY then
                delete_ready(self, id, task[i_utube])
            end
        end

        task = task:transform(i_status, 1, state.DONE)

        commit_func()

        if is_taken then
            return process_neighbour(self, task, 'delete')
        else
            self:on_task_change(task, 'delete')
            return task
        end
    end

    commit_func()
end

-- release task
function method.release(self, id, opts)
    local commit_func = begin_if_not_in_txn(self)

    local task = self.space:get{id}
    if task == nil then
        commit_func()
        return
    end
    if opts.delay ~= nil and opts.delay > 0 then
        task = self.space:update(id, {
            { '=', i_status, state.DELAYED },
            { '=', i_next_event, util.event_time(opts.delay) },
            { '+', i_ttl, util.time(opts.delay) }
        })
        if task ~= nil then
            if self.ready_space_mode then
                put_next_ready(self, task[i_utube])
            end

            commit_func()

            return process_neighbour(self, task, 'release')
        end
    else
        task = self.space:update(id, {
            { '=', i_status, state.READY },
            { '=', i_next_event, util.time(task[i_created] + task[i_ttl]) }
        })

        if self.ready_space_mode and task ~= nil then
            put_ready(self, task[i_id], task[i_utube], task[i_pri])
        end
    end

    commit_func()
    self:on_task_change(task, 'release')
    return task
end

-- bury task
function method.bury(self, id)
    local commit_func = begin_if_not_in_txn(self)

    -- The `i_next_event` should be updated because if the task has been
    -- "buried" after it was "taken" (and the task has "ttr") when the time in
    -- `i_next_event` will be interpreted as "ttl" in `utubettl_fiber_iteration`
    -- and the task will be deleted.
    local task = self.space:get{id}
    if task == nil then
        commit_func()
        return
    end

    local status = task[i_status]
    task = self.space:update(id, {
            { '=', i_status, state.BURIED },
            { '=', i_next_event, task[i_created] + task[i_ttl] }
    })
    if self.ready_space_mode then
        if status == state.READY then
            delete_ready(self, id, task[i_utube])
        elseif status == state.TAKEN then
            put_next_ready(self, task[i_utube])
        end
    end

    commit_func()

    return process_neighbour(
        self, task:transform(i_status, 1, state.BURIED), 'bury'
    )
end

-- unbury several tasks
function method.kick(self, count)
    for i = 1, count do
        local commit_func = begin_if_not_in_txn(self)

        local task = self.space.index.status:min{ state.BURIED }
        if task == nil then
            commit_func()

            return i - 1
        end
        if task[i_status] ~= state.BURIED then
            commit_func()

            return i - 1
        end

        task = self.space:update(task[i_id], {{ '=', i_status, state.READY }})
        if self.ready_space_mode then
            update_ready(self, task[i_id], task[i_utube], task[i_pri])
        end

        commit_func()

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
    if self.ready_space_mode then
        self.space_ready_buffer:truncate()
    end
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

function method.drop(self)
    self:stop()
    box.space[self.space.name]:drop()
    if self.ready_space_mode then
        box.space[self.space_ready_buffer.name]:drop()
    end
end

return tube
