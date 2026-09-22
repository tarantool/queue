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
local i_subqueue        = 8
local i_data            = 9

-- subqueue registry space: stores only unique subqueue names (single field).
-- The registry is best-effort: a write failure must never fail or roll back a
-- queue operation, and entries are never updated/deleted on physical deletes.
-- Stale names simply report zero counts in statistics().
local i_reg_subqueue    = 1

local function is_expired(task)
    local dead_event = task[i_created] + task[i_ttl]
    return (dead_event <= fiber.time64())
end

-- validate space of queue
local function validate_space(space)
    -- check indexes
    local indexes = {'task_id', 'status', 'watch', 'subqueue_pri'}
    for _, index in pairs(indexes) do
        if space.index[index] == nil then
            error(string.format('space "%s" does not have "%s" index',
                space.name, index))
        end
    end
end

-- validate subqueue registry space
local function validate_registry(space)
    if space.index['subqueue'] == nil then
        error(string.format('space "%s" does not have "subqueue" index',space.name))
    end
end

local function register_subqueue(registry, subqueue)
    -- Skip the write when the name is already known: an upsert with empty
    -- ops still produces a WAL record, which would double WAL traffic per put.
    if registry:get{subqueue} ~= nil then
        return
    end

    local ok, err = pcall(function()
        registry:upsert({subqueue}, {})
    end)
    if not ok then
        log.error('failed to register subqueue %s: %s', subqueue, tostring(err))
    end
end

-- create space
function tube.create_space(space_name, opts)
    if opts.engine == 'vinyl' then
        -- vinyl can not work properly in a competitive take. 
        -- The transaction may be aborted during taking.
        -- Without a transaction, the driver cannot guarantee that all 
        -- consumers will receive unique tasks.
        error('subqueuettl queue does not support vinyl engine')
    end

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
        {name = 'subqueue', type = str_type()},
        {name = 'data', type = '*'}
    }

    -- 1        2       3           4    5    6    7,       8      9
    -- task_id, status, next_event, ttl, ttr, pri, created, subqueue, data
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
    space:create_index('subqueue_pri', {
        type = 'tree',
        parts = {i_status, str_type(), i_subqueue, str_type(), i_pri, num_type(), i_id, num_type()}
    })
    return space
end

local delayed_state = { state.DELAYED }
local ttl_states    = { state.READY, state.BURIED }
local ttr_state     = { state.TAKEN }


local function subqueuettl_fiber_iteration(self, processed)
    local now       = util.time()
    local task      = nil
    local estimated = util.MAX_TIMEOUT

    -- delayed tasks
    task = util.atomic(function()
        local delayed_task = self.space.index.watch:min(delayed_state)
        if delayed_task == nil or delayed_task[i_status] ~= state.DELAYED then
            return nil
        end 

        if now < delayed_task[i_next_event] then
            estimated = tonumber(delayed_task[i_next_event] - now) / 1000000

            return nil
        end

        estimated = 0
        processed = processed + 1

        return self.space:update(delayed_task[i_id], {
            { '=', i_status, state.READY },
            { '=', i_next_event, delayed_task[i_created] + delayed_task[i_ttl] }
        })
    end)
    
    if task ~= nil then
        self:on_task_change(task, 'delayed')
    end

    -- ttl tasks
    for _, ttl_state in pairs(ttl_states) do
        task = self.space.index.watch:min{ ttl_state }
        if task ~= nil and task[i_status] == ttl_state then
            if now >= task[i_next_event] then
                task = self:delete(task[i_id])
                if task ~= nil then
                    self:on_task_change(task, 'ttl')
                end

                estimated = 0
                processed = processed + 1
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                estimated = et < estimated and et or estimated
            end
        end
    end

    -- ttr tasks
    task = util.atomic(function()
        local ttr_task = self.space.index.watch:min(ttr_state)
        if ttr_task == nil or ttr_task[i_status] ~= state.TAKEN then
            return nil
        end 

        if now < ttr_task[i_next_event] then
            local et = tonumber(ttr_task[i_next_event] - now) / 1000000
            estimated = et < estimated and et or estimated

            return nil
        end

        estimated = 0
        processed = processed + 1

        return self.space:update(ttr_task[i_id], {
            { '=', i_status, state.READY },
            { '=', i_next_event, ttr_task[i_created] + ttr_task[i_ttl] }
        })
    end)

    if task ~= nil then
        self:on_task_change(task, 'ttr')
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
local function subqueuettl_fiber(self)
    fiber.name('subqueuettl')
    log.info("Started queue subqueuettl fiber")
    local processed = 0

    while true do
        if box.info.ro == false then
            local stat, err = pcall(subqueuettl_fiber_iteration, self, processed)

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
                log.info("Queue subqueuettl fiber was stopped")
                break
            end
        end
    end
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    validate_space(space)

    -- Create or restore the best-effort subqueue registry. The registry
    -- stores ONLY unique subqueue names (no counters); statistics() iterates
    -- it and computes per-state counts on the fly via subqueue_pri:count().
    -- Registry writes are best-effort: failures never fail or roll back a
    -- queue operation, and entries are never removed on physical deletes, so
    -- stale names may remain and simply report zero counts in statistics().
    local registry_name = space.name .. '_subqueues'
    local registry = box.space[registry_name]
    if registry == nil then
        local registry_opts = {
            temporary = opts.temporary or false,
            engine    = opts.engine or 'memtx',
            format = {
                {name = 'subqueue', type = str_type()},
            },
        }
        registry = box.schema.create_space(registry_name, registry_opts)
        registry:create_index('subqueue', {
            type = 'tree',
            parts = {i_reg_subqueue, str_type()},
            unique = true,
        })
        -- Backfill names from existing tasks (e.g. after an upgrade or an
        -- unclean restart of a temporary registry). Best-effort: individual
        -- insert errors (duplicates, read-only, etc.) never block queue
        -- initialization.
        for _, task in space.index.subqueue_pri:pairs() do
            register_subqueue(registry, task[i_subqueue])
        end
    else
        validate_registry(registry)
    end

    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space              = space,
        registry           = registry,
        on_task_change     = function(self, task, stat_data)
            -- wakeup fiber
            if task ~= nil and self.fiber ~= nil then
                self.cond:signal(self.fiber:id())
            end
            on_task_change(task, stat_data)
        end,
        opts          = opts,
    }, { __index = method })

    self.cond  = qc.waiter()
    self.fiber = fiber.create(subqueuettl_fiber, self)
    self.sync_chan = fiber.channel(1)

    return self
end

-- method.grant grants provided user to all spaces of driver.
function method.grant(self, user, opts)
    box.schema.user.grant(user, 'read,write', 'space', self.space.name, opts)
    if self.registry ~= nil then
        box.schema.user.grant(user, 'read,write', 'space',
            self.registry.name, opts)
    end
end

function method.grant_role(self, role, opts)
    box.schema.role.grant(role, 'read,write', 'space', self.space.name, opts)
    if self.registry ~= nil then
        box.schema.role.grant(role, 'read,write', 'space',
            self.registry.name, opts)
    end
end

-- cleanup internal fields in task
function method.normalize_task(self, task)
    return task and task:transform(i_next_event, i_data - i_next_event)
end

-- put task in space
function method.put(self, data, opts)
    if opts.subqueue == nil then
        error('subqueue is required')
    end
    
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

    local subqueue = tostring(opts.subqueue)

    local task = util.atomic(function()
        local max = self.space.index.task_id:max()
        local id = max and max[i_id] + 1 or 0

        return self.space:insert{
            id,
            status,
            next_event,
            util.time(ttl),
            util.time(ttr),
            pri,
            util.time(),
            subqueue,
            data
        }
    end)

    -- Best-effort: record the subqueue name in the registry. Done outside
    -- the task transaction so a registry write failure (duplicate key,
    -- read-only, etc.) never fails or rolls back the put.
    register_subqueue(self.registry, subqueue)

    self:on_task_change(task, 'put')
    return task
end

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


local function take(self, subqueue)
    for _, task in self.space.index.subqueue_pri:pairs(
            {state.READY, subqueue}, {iterator = 'GE'}) do
        
        if task == nil 
            or task[i_status] ~= state.READY 
            or task[i_subqueue] ~= subqueue then
            break
        end
        if not is_expired(task) then
            task = self.space:update(task[i_id], {
                { '=', i_status, state.TAKEN },
                { '=', i_next_event, util.time() + task[i_ttr] }
            })

            if task ~= nil then
                return task
            end
        end
    end
end

-- take task
function method.take(self, opts)
    if opts == nil or opts.subqueue == nil then
        error('subqueue is required')
    end

    local task = util.atomic(function()
        return take(self, tostring(opts.subqueue))
    end)

    if task ~= nil then
        self:on_task_change(task, 'take')  
    end

    return task
end

function method.consumer_group(self, opts, task)
    local subqueue = task and task[i_subqueue] or opts and opts.subqueue

    return subqueue and 'subqueue:' .. tostring(subqueue) or nil
end

-- delete task
function method.delete(self, id)
    local task = util.atomic(function()
        local task = self.space:get(id)
        if task ~= nil then
            self.space:delete(id)
        end

        return task
    end)

    if task == nil then
        return nil
    end

    task = task:transform(i_status, 1, state.DONE)
    self:on_task_change(task, 'delete')

    return task
end

-- release task
function method.release(self, id, opts)
    local task = util.atomic(function()
        local task = self.space:get{id}
        if task == nil then
            return nil
        end

        if opts.delay ~= nil and opts.delay > 0 then
            return self.space:update(id, {
                { '=', i_status, state.DELAYED },
                { '=', i_next_event, util.event_time(opts.delay) },
                { '+', i_ttl, util.time(opts.delay) }
            })
        end

        return self.space:update(id, {
            { '=', i_status, state.READY },
            { '=', i_next_event, task[i_created] + task[i_ttl] }
        })
    end)

    if task == nil then
        return
    end

    self:on_task_change(task, 'release')

    return task
end

-- bury task
function method.bury(self, id)
    local task = util.atomic(function()
        -- The `i_next_event` should be updated because if the task has been
        -- "buried" after it was "taken" (and the task has "ttr") when the time in
        -- `i_next_event` will be interpreted as "ttl" in `subqueuettl_fiber_iteration`
        -- and the task will be deleted.
        local task = self.space:get{id}
        if task == nil then
            return nil
        end

        return self.space:update(id, {
            { '=', i_status, state.BURIED },
            { '=', i_next_event, task[i_created] + task[i_ttl] }
        })
    end)

    if task == nil then
        return
    end

    task = task:transform(i_status, 1, state.BURIED)
    self:on_task_change(task, 'bury')

    return task
end

-- unbury several tasks
function method.kick(self, count)
    for i = 1, count do
        local task = util.atomic(function()
            local task = self.space.index.status:min{ state.BURIED }
            if task == nil or task[i_status] ~= state.BURIED then
                return nil
            end

            return self.space:update(task[i_id], {{ '=', i_status, state.READY }})
        end)        
        if task == nil then
            return i - 1
        end

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

function method.statistics(self)
    local statistics = {}
    -- Iterate the registry of unique subqueue names. Per-state counts are
    -- computed on the fly via subqueue_pri:count(); stale names (whose last
    -- task was deleted/acked) simply report zero counts.
    for _, r in self.registry:pairs() do
        local subqueue = r[i_reg_subqueue]
        local stats = {total = 0}
        for name, task_state in pairs(state) do
            if task_state ~= state.DONE then
                local count = self.space.index.subqueue_pri:count{
                    task_state, subqueue,
                }
                stats[name:lower()] = count
                stats.total = stats.total + count
            end
        end

        statistics[subqueue] = stats
    end

    return {subqueues = statistics}
end

function method.truncate(self)
    self.space:truncate()
    if self.registry ~= nil then
        self.registry:truncate()
    end
end

function method.start(self)
    if self.fiber then
        return
    end

    self.fiber = fiber.create(subqueuettl_fiber, self)
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
    if self.registry ~= nil and box.space[self.registry.name] ~= nil then
        box.space[self.registry.name]:drop()
    end
end

return tube
