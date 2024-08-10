local state    = require('queue.abstract.state')
local num_type = require('queue.compat').num_type
local str_type = require('queue.compat').str_type

local tube = {}
local method = {}

tube.STORAGE_MODE_DEFAULT = "default"
tube.STORAGE_MODE_READY_BUFFER = "ready_buffer"

local i_status = 2

-- validate space of queue
local function validate_space(space)
    -- check indexes
    local indexes = {'task_id', 'status', 'utube'}
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
    local indexes = {'task_id', 'utube'}
    for _, index in pairs(indexes) do
        if space.index[index] == nil then
            error(string.format('space "%s" does not have "%s" index',
                    space.name, index))
        end
    end
end

-- create space
function tube.create_space(space_name, opts)
    local space_opts         = {}
    local if_not_exists      = opts.if_not_exists or false
    space_opts.temporary     = opts.temporary or false
    space_opts.engine        = opts.engine or 'memtx'
    space_opts.format = {
        {name = 'task_id', type = num_type()},
        {name = 'status', type = str_type()},
        {name = 'utube', type = str_type()},
        {name = 'data', type = '*'}
    }

    -- id, status, utube, data
    local space = box.space[space_name]
    if if_not_exists and space then
        -- Validate the existing space.
        validate_space(box.space[space_name])
        return space
    end

    space = box.schema.create_space(space_name, space_opts)
    space:create_index('task_id', {
        type = 'tree',
        parts = {1, num_type()}
    })
    space:create_index('status', {
        type = 'tree',
        parts = {2, str_type(), 1, num_type()}
    })
    space:create_index('utube', {
        type = 'tree',
        parts = {2, str_type(), 3, str_type(), 1, num_type()}
    })
    return space
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    validate_space(space)

    local space_opts         = {}
    space_opts.temporary     = opts.temporary or false
    space_opts.engine        = opts.engine or 'memtx'
    space_opts.format = {
        {name = 'task_id', type = num_type()},
        {name = 'utube', type = str_type()}
    }

    local space_ready_buffer_name = space.name .. "_ready_buffer"
    local space_ready_buffer = box.space[space_ready_buffer_name]

    local ready_space_mode = (opts.storage_mode == tube.STORAGE_MODE_READY_BUFFER)
    if ready_space_mode then
        if space_ready_buffer == nil then
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
        on_task_change     = on_task_change,
        ready_space_mode   = ready_space_mode,
        opts               = opts,
    }, { __index = method })
    return self
end

-- normalize task: cleanup all internal fields
function method.normalize_task(self, task)
    return task and task:transform(3, 1)
end

-- Find the first ready task for given 'utube'.
-- Utube is also checked for the absence of 'TAKEN' tasks.
local function put_next_ready(self, utube)
    local taken = self.space.index.utube:min{state.TAKEN, utube}
    if taken == nil or taken[2] ~= state.TAKEN then
        local next_task = self.space.index.utube:min{state.READY, utube}
        if next_task == nil or next_task[2] ~= state.READY then
            return
        end
        -- Ignoring ER_TUPLE_FOUND error, if a tuple with the same task_id
        -- or utube name is already in the space.
        -- Note that both task_id and utube indexes are unique, so there will be
        -- no duplicates: each task_id can occur in the space not more than once,
        -- there can be no more than one task from each utube in a space.
        pcall(self.space_ready_buffer.insert, self.space_ready_buffer, {next_task[1], utube})
    end
end

-- Put this task into ready_buffer.
-- Utube is also checked for the absence of 'TAKEN' tasks.
local function put_ready(self, id, utube)
    local taken = self.space.index.utube:min{state.TAKEN, utube}
    if taken == nil or taken[2] ~= state.TAKEN then
        -- Ignoring ER_TUPLE_FOUND error, if a tuple with the same task_id
        -- or utube name is already in the space.
        -- Note that both task_id and utube indexes are unique, so there will be
        -- no duplicates: each task_id can occur in the space not more than once,
        -- there can be no more than one task from each utube in a space.
        pcall(self.space_ready_buffer.insert, self.space_ready_buffer, {id, utube})
    end
end

local function commit()
    box.commit()
end

local function rollback()
    box.rollback()
end

local function empty()
end

-- Start transaction with the correct options, if the transaction is not already running.
local function begin_if_not_in_txn()
    local transaction_opts = {}
    if box.cfg.memtx_use_mvcc_engine then
        transaction_opts = {txn_isolation = 'read-committed'}
    end

    if not box.is_in_txn() then
        box.begin(transaction_opts)
        return commit, rollback
    else
        return empty, empty
    end
end

-- Try commiting operations until success. This is required for 'vinyl' engine.
-- In case of a transaction conflict for 'vinyl' we need to retry an entire
-- transaction.
local function try_commit_several_times(func, ...)
    local ok = false
    local ret
    while not ok do
        local commit_func, rollback_func = begin_if_not_in_txn()
        ok, ret = pcall(func, commit_func, ...)
        if ok then
            return ret
        end
        rollback_func()
        require('fiber').yield()
    end
end

-- put task in space
local function put(self, data, opts, commit_func)
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
    local max = self.space.index.task_id:max()

    local id = max and max[1] + 1 or 0
    local task = self.space:insert{id, state.READY, tostring(opts.utube), data}
    if self.ready_space_mode then
        put_ready(self, task[1], task[3])
    end

    commit_func()

    self.on_task_change(task, 'put')
    return task
end

-- put task in space
function method.put(self, data, opts)
    local commit_body = function(commit_func)
        return put(self, data, opts, commit_func)
    end

    return try_commit_several_times(commit_body)
end

-- Take the first task form the ready_buffer.
local function take_ready(self, commit_func)
    while true do
        local task_ready = self.space_ready_buffer.index.task_id:min()
        if task_ready == nil then
            commit_func()
            return nil
        end

        local id = task_ready[1]
        local task = self.space:get(id)
        local take_complete = false

        if task[2] == state.READY then
            local taken = self.space.index.utube:min{state.TAKEN, task[3]}

            if taken == nil or taken[2] ~= state.TAKEN then
                task = self.space:update(id, { { '=', 2, state.TAKEN } })
                self.space_ready_buffer:delete(id)
                take_complete = true
            end
        end

        commit_func()

        if take_complete then
            self.on_task_change(task, 'take')
            return task
        end
    end
end

local function take_step(self, task, commit_func)
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
    local taken = self.space.index.utube:min{state.TAKEN, task[3]}
    local take_complete = false

    if taken == nil or taken[2] ~= state.TAKEN then
        task = self.space:update(task[1], { { '=', 2, state.TAKEN } })
        take_complete = true
    end

    commit_func()
    if take_complete then
        self.on_task_change(task, 'take')
        return task
    end
end

-- take task
function method.take(self)
    if self.ready_space_mode then
        local commit_body = function(commit_func)
            return take_ready(self, commit_func)
        end

        return try_commit_several_times(commit_body)
    end

    for _, task in self.space.index.status:pairs(state.READY,
            { iterator = 'GE' }) do
        if task[2] ~= state.READY then
            break
        end

        local commit_body = function(commit_func)
            return take_step(self, task, commit_func)
        end

        local ret = try_commit_several_times(commit_body)
        if ret ~= nil then
            return ret
        end
    end
end

-- touch task
function method.touch(self, id, ttr)
    error('utube queue does not support touch')
end

-- Delete task from the ready_buffer and find next ready task from the same 'utube' to replace it.
local function delete_ready(self, id, utube)
    self.space_ready_buffer:delete(id)
    put_next_ready(self, utube)
end

-- delete task
local function delete(self, id, commit_func)
    local task = self.space:get(id)
    self.space:delete(id)
    if task ~= nil then
        if self.ready_space_mode then
            if task[2] == state.TAKEN then
                put_next_ready(self, task[3])
            elseif task[2] == state.READY then
                delete_ready(self, id, task[3])
            end
        end

        task = task:transform(2, 1, state.DONE)

        local neighbour = self.space.index.utube:min{state.READY, task[3]}

        commit_func()

        self.on_task_change(task, 'delete')
        if neighbour then
            self.on_task_change(neighbour)
        end
        return task
    end

    commit_func()
    return task
end

-- delete task
function method.delete(self, id)
    local commit_body = function(commit_func)
        return delete(self, id, commit_func)
    end

    return try_commit_several_times(commit_body)
end

-- release task
local function release(self, id, opts, commit_func)
    local task = self.space:update(id, {{ '=', 2, state.READY }})
    if task ~= nil then
        if self.ready_space_mode then
            local inserted, err =
                pcall(self.space_ready_buffer.insert, self.space_ready_buffer, {id, task[3]})
            if not inserted then
                require('log').warn(
                        'queue: [tube "utube"] insert after release error: %s', err)
                delete_ready(self, task[1], task[3])
            end
        end

        commit_func()

        self.on_task_change(task, 'release')
        return task
    end

    commit_func()
    return task
end

-- release task
function method.release(self, id, opts)
    local commit_body = function(commit_func)
        return release(self, id, opts, commit_func)
    end

    return try_commit_several_times(commit_body)
end

-- bury task
local function bury(self, id, commit_func)
    local current_task = self.space:get{id}
    local task = self.space:update(id, {{ '=', 2, state.BURIED }})
    if task ~= nil then
        if self.ready_space_mode then
            local status = current_task[2]
            local ready_task = self.space_ready_buffer:get{task[1]}
            if ready_task ~= nil then
                delete_ready(self, id, task[3])
            elseif status == state.TAKEN then
                put_next_ready(self, task[3])
            end
        end

        local neighbour = self.space.index.utube:min{state.READY, task[3]}

        commit_func()

        self.on_task_change(task, 'bury')
        if neighbour and neighbour[i_status] == state.READY then
            self.on_task_change(neighbour)
        end
    else
        commit_func()

        self.on_task_change(task, 'bury')
    end
    return task
end

-- bury task
function method.bury(self, id)
    local commit_body = function(commit_func)
        return bury(self, id, commit_func)
    end

    return try_commit_several_times(commit_body)
end

-- unbury several tasks
local function kick_step(self, id, commit_func)
    local task = self.space.index.status:min{ state.BURIED }
    if task == nil then
        return id - 1
    end
    if task[2] ~= state.BURIED then
        return id - 1
    end

    task = self.space:update(task[1], {{ '=', 2, state.READY }})
    if self.ready_space_mode then
        local prev_task = self.space_ready_buffer.index.utube:get{task[3]}
        if prev_task ~= nil then
            if prev_task[1] > task[1] then
                self.space_ready_buffer:delete(prev_task[1])
                self.space_ready_buffer:insert({task[1], task[2]})
            end
        else
            put_ready(self, task[3])
        end
    end

    commit_func()

    self.on_task_change(task, 'kick')
end

-- unbury several tasks
function method.kick(self, count)
    for i = 1, count do
        local commit_body = function(commit_func)
            return kick_step(self, i, commit_func)
        end

        local ret = try_commit_several_times(commit_body)
        if ret ~= nil then
            return ret
        end
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

-- This driver has no background activity.
-- Implement dummy methods for the API requirement.
function method.start()
    return
end

function method.stop()
    return
end

function method.drop(self)
    self:stop()
    box.space[self.space.name]:drop()
    if self.ready_space_mode then
        box.space[self.space_ready_buffer.name]:drop()
    end
end

return tube
