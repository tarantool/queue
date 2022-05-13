local log      = require('log')
local fiber    = require('fiber')
local uuid     = require('uuid')

local session  = require('queue.abstract.queue_session')
local state    = require('queue.abstract.state')
local queue_state = require('queue.abstract.queue_state')

local util     = require('queue.util')
local qc       = require('queue.compat')
local num_type = qc.num_type
local str_type = qc.str_type

-- The term "queue session" has been added to the queue. One "queue session"
-- can include many connections (box.session). For clarity, the box.session
-- will be referred to as connection below.
local connection = box.session

local queue = {
    tube = setmetatable({}, {
        __call = function(self, tube_name)
            if tube_name and type(tube_name) ~= 'string' then
                error('argument #1 should be string or nil')
            end
            -- return all names of tubes
            if tube_name == nil then
                local rv = {}
                for name, _ in pairs(self) do
                    table.insert(rv, name)
                end
                return rv
            end
            -- return true/false if tube name is provided
            return not (self[tube_name] == nil)
        end
    }),
    stat = {}
}

-- Release tasks that don't have session_uuid stored in inactive sessions.
local function tube_release_all_orphaned_tasks(tube)
    local prefix = ('queue: [tube "%s"] '):format(tube.name)

    -- We lean on stable iterators in this function.
    -- https://github.com/tarantool/tarantool/issues/1796
    if not qc.check_version({1, 7, 5}) then
        log.error(prefix .. 'no stable iterator support: skip task releasing')
        log.error(prefix .. 'some tasks may stuck in taken state perpetually')
        log.error(prefix .. 'update tarantool to >= 1.7.5 or take the risk')
        return
    end

    log.info(prefix .. 'releasing all taken task (may take a while)')
    local released = 0
    for _, task in tube.raw:tasks_by_state(state.TAKEN) do
        local taken = box.space._queue_taken_2.index.task:get{
            tube.tube_id, task[1]
        }
        if taken and session.exist_shared(taken[4]) then
            log.info(prefix ..
                ('skipping task: %d, tube_id: %d'):format(task[1],
                    tube.tube_id))
        else
            tube.raw:release(task[1], {})
            released = released + 1
        end
    end
    log.info(prefix .. ('released %d tasks'):format(released))
end

--- Check whether the task has been taken in a current session or in a session
-- with session uuid = session_uuid.
-- Throw an error if task is not take in the session.
local function check_task_is_taken(tube_id, task_id, session_uuid)
    local _taken = box.space._queue_taken_2.index.task:get{tube_id, task_id}

    session_uuid = session_uuid or session.identify(connection.id())
    if _taken == nil or _taken[4] ~= session_uuid then
        error("Task was not taken")
    end
end

-- tube methods
local tube = {}

-- This check must be called from all public tube methods.
local function check_state()
    if queue_state.get() ~= queue_state.states.RUNNING then
        log.error(('Queue is in %s state'):format(queue_state.show()))
        return false
    end

    return true
end

function tube.put(self, data, opts)
    if not check_state() then
        return nil
    end
    opts = opts or {}
    local task = self.raw:put(data, opts)
    return self.raw:normalize_task(task)
end

local conds = {}
local releasing_connections = {}

function tube.take(self, timeout)
    if not check_state() then
        return nil
    end
    timeout = util.time(timeout or util.TIMEOUT_INFINITY)
    local task = self.raw:take()
    if task ~= nil then
        return self.raw:normalize_task(task)
    end

    while timeout > 0 do
        local started = fiber.time64()
        local time = util.event_time(timeout)
        local tid = self.tube_id
        local fid = fiber.id()
        local conn_id = connection.id()

        box.space._queue_consumers:insert{conn_id, fid, tid, time, started}
        conds[fid] = qc.waiter()
        conds[fid]:wait(tonumber(timeout) / 1000000)
        conds[fid]:free()
        box.space._queue_consumers:delete{conn_id, fid}

        -- We don't take a task if the connection is in a
        -- disconnecting state.
        if releasing_connections[fid] then
            releasing_connections[fid] = nil
            return nil
        end

        task = self.raw:take()

        if task ~= nil then
            return self.raw:normalize_task(task)
        end

        local elapsed = fiber.time64() - started
        timeout = timeout > elapsed and timeout - elapsed or 0
    end
end

function tube.touch(self, id, delta)
    if not check_state() then
        return
    end
    if delta == nil then
        return
    end
    if delta < 0 then -- if delta is lesser then 0, then it's zero
        delta = 0
    elseif delta > util.MAX_TIMEOUT then -- no ttl/ttr for this task
        delta = util.TIMEOUT_INFINITY
    else -- convert to usec
        delta = delta * 1000000
    end
    if delta == 0 then
        return
    end

    check_task_is_taken(self.tube_id, id)

    local space_name = box.space._queue:get{self.name}[3]
    queue.stat[space_name]:inc('touch')

    return self.raw:normalize_task(self.raw:touch(id, delta))
end

function tube.ack(self, id)
    if not check_state() then
        return nil
    end
    check_task_is_taken(self.tube_id, id)
    local tube = box.space._queue:get{self.name}
    local space_name = tube[3]

    self:peek(id)
    -- delete task
    box.space._queue_taken_2.index.task:delete{self.tube_id, id}
    local result = self.raw:normalize_task(
        self.raw:delete(id):transform(2, 1, state.DONE)
    )
    -- swap delete and ack call counters
    queue.stat[space_name]:inc('ack')
    queue.stat[space_name]:dec('delete')
    return result
end

local function tube_release_internal(self, id, opts, session_uuid)
    opts = opts or {}
    check_task_is_taken(self.tube_id, id, session_uuid)

    box.space._queue_taken_2.index.task:delete{self.tube_id, id}
    self:peek(id)
    return self.raw:normalize_task(self.raw:release(id, opts))
end

function tube.release(self, id, opts)
    if not check_state() then
        return nil
    end
    return tube_release_internal(self, id, opts)
end

-- Release all tasks.
function tube.release_all(self)
    if not check_state() then
        return
    end
    local prefix = ('queue: [tube "%s"] '):format(self.name)

    log.info(prefix .. 'releasing all taken task (may take a while)')
    local released = 0
    for _, task in self.raw:tasks_by_state(state.TAKEN) do
        self.raw:release(task[1], {})
        released = released + 1
    end
    log.info(('%s released %d tasks'):format(prefix, released))
end

function tube.peek(self, id)
    if not check_state() then
        return nil
    end
    local task = self.raw:peek(id)
    if task == nil then
        error(("Task %s not found"):format(tostring(id)))
    end
    return self.raw:normalize_task(task)
end

function tube.bury(self, id)
    if not check_state() then
        return nil
    end
    local task = self:peek(id)
    local is_taken, _ = pcall(check_task_is_taken, self.tube_id, id)
    if is_taken then
        box.space._queue_taken_2.index.task:delete{self.tube_id, id}
    end
    if task[2] == state.BURIED then
        return task
    end
    return self.raw:normalize_task(self.raw:bury(id))
end

function tube.kick(self, count)
    if not check_state() then
        return nil
    end
    count = count or 1
    return self.raw:kick(count)
end

function tube.delete(self, id)
    if not check_state() then
        return nil
    end
    self:peek(id)
    return self.raw:normalize_task(self.raw:delete(id))
end

-- drop tube
function tube.drop(self)
    if not check_state() then
        return nil
    end
    local tube_name = self.name

    local tube = box.space._queue:get{tube_name}
    if tube == nil then
        error("Tube not found")
    end

    local tube_id = tube[2]

    local cons = box.space._queue_consumers.index.consumer:min{tube_id}

    if cons ~= nil and cons[3] == tube_id then
        error("There are consumers connected the tube")
    end

    local taken = box.space._queue_taken_2.index.task:min{tube_id}
    if taken ~= nil and taken[1] == tube_id then
        error("There are taken tasks in the tube")
    end

    local space_name = tube[3]

    box.space[space_name]:drop()
    box.space._queue:delete{tube_name}
    -- drop queue
    queue.tube[tube_name] = nil
    -- drop statistics
    queue.stat[tube_name] = nil
    return true
end

-- truncate tube
-- (delete everything from tube)
function tube.truncate(self)
    if not check_state() then
        return
    end
    self.raw:truncate()
end

function tube.on_task_change(self, cb)
    local old_cb = self.on_task_change_cb
    self.on_task_change_cb = cb or (function() end)
    return old_cb
end

function tube.grant(self, user, args)
    if not check_state() then
        return
    end
    local function tube_grant_space(user, name, tp)
        box.schema.user.grant(user, tp or 'read,write', 'space', name, {
            if_not_exists = true,
        })
    end

    local function tube_grant_func(user, name)
        box.schema.func.create(name, { if_not_exists = true })
        box.schema.user.grant(user, 'execute', 'function', name, {
            if_not_exists = true
        })
    end

    args = args or {}

    tube_grant_space(user, '_queue', 'read')
    tube_grant_space(user, '_queue_consumers')
    tube_grant_space(user, '_queue_taken_2')
    tube_grant_space(user, self.name)
    session.grant(user)

    if args.call then
        tube_grant_func(user, 'queue.identify')
        tube_grant_func(user, 'queue.statistics')
        local prefix = (args.prefix or 'queue.tube') .. ('.%s:'):format(self.name)
        tube_grant_func(user, prefix .. 'put')
        tube_grant_func(user, prefix .. 'take')
        tube_grant_func(user, prefix .. 'touch')
        tube_grant_func(user, prefix .. 'ack')
        tube_grant_func(user, prefix .. 'release')
        tube_grant_func(user, prefix .. 'peek')
        tube_grant_func(user, prefix .. 'bury')
        tube_grant_func(user, prefix .. 'kick')
        tube_grant_func(user, prefix .. 'delete')
    end

    if args.truncate then
        local prefix = (args.prefix or 'queue.tube') .. ('.%s:'):format(self.name)
        tube_grant_func(user, prefix .. 'truncate')
    end

end

-- methods
local method = {}

-- List of required driver methods.
local required_driver_methods = {
    'normalize_task',
    'put',
    'take',
    'delete',
    'release',
    'bury',
    'kick',
    'peek',
    'touch',
    'truncate',
    'tasks_by_state'
}

-- gh-126 Check the driver API.
local function check_driver_api(tube_impl, tube_type)
    for _, v in pairs(required_driver_methods) do
        if tube_impl[v] == nil then
            error(string.format('The "%s" driver does not have an ' ..
                                'implementation of method "%s".', tube_type, v))
        end
    end
end

-- Cache of already verified drivers.
local checked_drivers = {}

local function make_self(driver, space, tube_name, tube_type, tube_id, opts)
    opts = opts or {}
    local self

    -- wakeup consumer if queue have new task
    local on_task_change = function(task, stats_data)
        self.on_task_change_cb(task, stats_data)

        -- task was removed
        if task == nil then return end

        -- We cannot use a local variable to access the space `_queue_taken_2`
        -- because it can be recreated in `switch_in_replicaset()`.
        local queue_consumers = box.space._queue_consumers

        -- if task was taken and become other state
        local taken = box.space._queue_taken_2.index.task:get{tube_id, task[1]}
        if taken ~= nil then
            box.space._queue_taken_2:delete{taken[1], taken[2]}
        end
        -- task switched to ready (or new task)
        if task[2] == state.READY then
            local tube_id = self.tube_id
            local consumer = queue_consumers.index.consumer:min{tube_id}

            if consumer ~= nil then
                if consumer[3] == tube_id then
                    queue_consumers:delete{consumer[1], consumer[2]}
                    local cond = conds[consumer[2]]
                    if cond then
                        cond:signal(consumer[2])
                    end
                end
            end
        -- task switched to taken - register in taken space
        elseif task[2] == state.TAKEN then
            local conn_id = connection.id()
            local session_uuid = session.identify(conn_id)
            box.space._queue_taken_2:insert{
                self.tube_id,
                task[1],
                conn_id,
                session_uuid,
                fiber.time64()
            }
        end
        if stats_data ~= nil then
            queue.stat[space.name]:inc(stats_data)
        end
        if stats_data == 'delete' then
            queue.stat[space.name]:inc('done')
        end
    end

    self = setmetatable({
        raw     = driver.new(space, on_task_change, opts),
        name    = tube_name,
        type    = tube_type,
        tube_id = tube_id,
        opts    = opts,
    }, {
        __index = tube
    })

    if checked_drivers[tube_type] == nil then
        check_driver_api(self.raw, tube_type)
        checked_drivers[tube_type] = true
    end

    self:on_task_change(opts.on_task_change)
    queue.tube[tube_name] = self

    return self
end

--- Release all session tasks.
local function release_session_tasks(session_uuid)
    local taken_tasks = box.space._queue_taken_2.index.uuid:select{session_uuid}

    for _, task in pairs(taken_tasks) do
        local tube = box.space._queue.index.tube_id:get{task[1]}
        if tube == nil then
            log.error("Inconsistent queue state: tube %d not found", task[1])
            box.space._queue_taken_2.index.task:delete{task[1], task[2]}
        else
            log.warn("Session %s closed, release task %s(%s)",
                uuid.frombin(session_uuid):str(), task[2], tube[1])
            tube_release_internal(queue.tube[tube[1]], task[2], nil,
                session_uuid)
        end
    end
end

function method.state()
    return queue_state.show()
end

function method._on_consumer_disconnect()
    local conn_id = connection.id()

    -- wakeup all waiters
    while true do
        local waiter = box.space._queue_consumers.index.pk:min{conn_id}
        if waiter == nil then
            break
        end
        -- Don't touch the other consumers
        if waiter[1] ~= conn_id then
            break
        end
        box.space._queue_consumers:delete{waiter[1], waiter[2]}
        local cond = conds[waiter[2]]
        if cond then
            releasing_connections[waiter[2]] = true
            cond:signal(waiter[2])
        end
    end

    session.disconnect(conn_id)
end

-- Function takes new queue state.
-- The "RUNNING" and "WAITING" states do not require additional actions.
local function on_state_change(state)
    if state == queue_state.states.STARTUP then
        for name, tube in pairs(queue.tube) do
            tube_release_all_orphaned_tasks(tube)
            log.info('queue: [tube "%s"] start driver', name)
            if not tube.raw.start then
                log.warn('queue: [tube "%s"] method start is not implemented',
                    tube.name)
            else
                tube.raw:start()
            end
        end
        session.start()
    elseif state == queue_state.states.ENDING then
        for name, tube in pairs(queue.tube) do
            log.info('queue: [tube "%s"] stop driver', name)
            if not tube.raw.stop then
                log.warn('queue: [tube "%s"] method stop is not implemented',
                    tube.name)
            else
                tube.raw:stop()
            end
        end
        session.stop()
    else
        error('on_state_change: unexpected queue state')
    end
end

-- function takes tuples and recreates tube
local function recreate_tube(tube_tuple)
    local name, id, space_name, tube_type, opts = tube_tuple:unpack()

    local driver = queue.driver[tube_type]
    if driver == nil then
        error("Unknown tube type " .. tostring(tube_type))
    end

    local space = box.space[space_name]
    if space == nil then
        error(("Space '%s' doesn't exists"):format(space_name))
    end
    return make_self(driver, space, name, tube_type, id, opts)
end

-------------------------------------------------------------------------------
-- create tube
function method.create_tube(tube_name, tube_type, opts)
    if not check_state() then
        return
    end
    opts = opts or {}
    if opts.if_not_exists == nil then
        opts.if_not_exists = false
    end
    if opts.if_not_exists == true and queue.tube[tube_name] ~= nil then
        return queue.tube[tube_name]
    end

    local replicaset_mode = queue.cfg['in_replicaset'] or false
    if replicaset_mode and opts.temporary then
        log.error("Cannot create temporary tube in replicaset mode")
        return
    end

    local driver = queue.driver[tube_type]
    if driver == nil then
        error("Unknown tube type " .. tostring(tube_type))
    end
    -- space name must be equal to tube name
    -- https://github.com/tarantool/queue/issues/9#issuecomment-83019109
    local space_name = tube_name
    if box.space[space_name] ~= nil and opts.if_not_exists == false then
        error(("Space '%s' already exists"):format(space_name))
    end

    -- if tube tuple was already presented, then recreate old tube
    local ptube = box.space._queue:get{tube_name}
    if ptube ~= nil then
        local self = recreate_tube(ptube)
        self:on_task_change(opts.on_task_change)
        return self
    end

    -- create tube space
    local space = driver.create_space(space_name, opts)

    -- create tube record
    local last = box.space._queue.index.tube_id:max()
    local tube_id = 0
    if last ~= nil then
        tube_id = last[2] + 1
    end
    local self = make_self(driver, space, tube_name, tube_type, tube_id, opts)
    opts.on_task_change = nil
    box.space._queue:insert{tube_name, tube_id, space_name, tube_type, opts}
    return self
end

--
-- Replicaset mode switch.
--
-- Running a queue in master-replica mode requires that
-- `_queue_taken_2` space was not temporary.
-- When the queue is running in single mode,
-- the space is converted to temporary mode to increase performance.
--
local function switch_in_replicaset(replicaset_mode)
    if replicaset_mode == nil then
        log.warn('queue: queue required after box.cfg{}')
        replicaset_mode = false
    end

    if not box.space._queue_taken_2 then
        return
    end

    if box.space._queue_taken_2.temporary and replicaset_mode == false then
        return
    end

    if not box.space._queue_taken_2.temporary and replicaset_mode == true then
        return
    end

    box.schema.create_space('_queue_taken_2_mgr', {
        temporary = not replicaset_mode,
        format = {
            {name = 'tube_id', type = num_type()},
            {name = 'task_id', type = num_type()},
            {name = 'connection_id', type = num_type()},
            {name = 'session_uuid', type = str_type()},
            {name = 'taken_time', type = num_type()}
        }})

    box.space._queue_taken_2_mgr:create_index('task', {
        type = 'tree',
        parts = {1, num_type(), 2, num_type()},
        unique = true
    })
    box.space._queue_taken_2_mgr:create_index('uuid', {
        type = 'tree',
        parts = {4, str_type()},
        unique = false
    })

    box.begin() -- Disable implicit yields until the transaction ends.
    for _, tuple in box.space._queue_taken_2:pairs() do
        box.space._queue_taken_2_mgr:insert(tuple)
    end

    box.space._queue_taken_2:drop()
    box.space._queue_taken_2_mgr:rename('_queue_taken_2')

    local status, err = pcall(box.commit)
    if not status then
        error(('Error migrate _queue_taken_2: %s'):format(tostring(err)))
    end
end

-- create or join infrastructure
function method.start()
    -- tube_name, tube_id, space_name, tube_type, opts
    local _queue = box.space._queue
    if _queue == nil then
        _queue = box.schema.create_space('_queue', {
            format = {
                {name = 'tube_name', type = str_type()},
                {name = 'tube_id', type = num_type()},
                {name = 'space_name', type = str_type()},
                {name = 'tube_type', type = str_type()},
                {name = 'opts', type = '*'}
            }
        })
        _queue:create_index('tube', {
            type = 'tree',
            parts = {1, str_type()},
            unique = true
        })
        _queue:create_index('tube_id', {
            type = 'tree',
            parts = {2, num_type()},
            unique = true
        })
    end

    local _cons = box.space._queue_consumers
    if _cons == nil then
        -- connection, fid, tube, time
        _cons = box.schema.create_space('_queue_consumers', {
            temporary = true,
            format = {
                {name = 'connection_id', type = num_type()},
                {name = 'fiber_id', type = num_type()},
                {name = 'tube_id', type = num_type()},
                {name = 'event_time', type = num_type()},
                {name = 'fiber_time', type = num_type()}
            }
        })
        _cons:create_index('pk', {
            type = 'tree',
            parts = {1, num_type(), 2, num_type()},
            unique = true
        })
        _cons:create_index('consumer', {
            type = 'tree',
            parts = {3, num_type(), 4, num_type()},
            unique = false
        })
    end

    -- Remove deprecated space
    if box.space._queue_taken ~= nil then
        box.space._queue_taken:drop()
    end

    local replicaset_mode = queue.cfg['in_replicaset'] or false
    if box.space._queue_taken_2 == nil then
        -- tube_id, task_id, connection_id, session_uuid, time
        box.schema.create_space('_queue_taken_2', {
            temporary = not replicaset_mode,
            format = {
                {name = 'tube_id', type = num_type()},
                {name = 'task_id', type = num_type()},
                {name = 'connection_id', type = num_type()},
                {name = 'session_uuid', type = str_type()},
                {name = 'taken_time', type = num_type()}
            }})

        box.space._queue_taken_2:create_index('task', {
            type = 'tree',
            parts = {1, num_type(), 2, num_type()},
            unique = true
        })
        box.space._queue_taken_2:create_index('uuid', {
            type = 'tree',
            parts = {4, str_type()},
            unique = false
        })
    else
        switch_in_replicaset(queue.cfg['in_replicaset'])
    end

    for _, tube_tuple in _queue:pairs() do
        -- Recreate tubes for registered drivers only.
        -- Check if a driver exists for this type of tube.
        if queue.driver[tube_tuple[4]] ~= nil then
            local tube = recreate_tube(tube_tuple)
            -- gh-66: release all taken tasks on start
            tube_release_all_orphaned_tasks(tube)
        end
    end

    session.on_session_remove(release_session_tasks)
    session.start()

    connection.on_disconnect(queue._on_consumer_disconnect)
    queue_state.init(on_state_change)
    return queue
end

--- Register the custom driver.
-- Unlike the "register_driver" method from init.lua, this method
-- recreates the existing tubes of the registered driver.
function method.register_driver(driver_name, tube_ctr)
    if type(tube_ctr.create_space) ~= 'function' or
        type(tube_ctr.new) ~= 'function' then
        error('tube control methods must contain functions "create_space"'
              .. ' and "new"')
    end
    if queue.driver[driver_name] then
        error(('overriding registered driver "%s"'):format(driver_name))
    end
    queue.driver[driver_name] = tube_ctr

    -- Recreates the existing tubes of the registered driver.
    local _queue = box.space._queue
    for _, tube_tuple in _queue:pairs() do
        if tube_tuple[4] == driver_name then
            local tube = recreate_tube(tube_tuple)
            -- Release all task for tube on start.
            tube_release_all_orphaned_tasks(tube)
        end
    end
end

local function build_stats(space)
    local stats = {tasks = {}, calls = {
        ack  = 0, bury  = 0, delete  = 0,
        kick = 0, put   = 0, release = 0,
        take = 0, touch = 0,
        -- for *ttl queues only
        ttl  = 0, ttr   = 0, delay   = 0,
    }}

    local st = rawget(queue.stat, space) or {}
    local idx_tube = 1

    -- add api calls stats
    for name, value in pairs(st) do
        if type(value) ~= 'function' and name ~= 'done' then
            stats['calls'][name] = value
        end
    end

    local total = 0
    -- add tasks by state count
    for i, s in pairs(state) do
        local st = i:lower()
        stats['tasks'][st] = box.space[space].index[idx_tube]:count(s)
        total = total + stats['tasks'][st]
    end

    -- add total tasks count
    stats['tasks']['total'] = total
    stats['tasks']['done'] = st.done or 0

    return stats
end

--- Identifies the connection and return the UUID of the current session.
-- If session_uuid ~= nil: associate the connection with given session.
function method.identify(session_uuid)
    return session.identify(connection.id(), session_uuid)
end

--- Configure of the queue module.
-- If an invalid value or an unknown option
-- is used, an error will be thrown.
local function cfg(self, opts)
    opts = opts or {}
    local session_opts = {}

    -- Set default in_replicaset value.
    if opts['in_replicaset'] == nil then
        opts['in_replicaset'] = false
    end

    -- Temporary spaces are not allowed in replicaset mode.
    if opts['in_replicaset'] == true and box.space._queue then
        local temp_tubes = ""
        for _, tube in box.space._queue:pairs() do
            if tube[5].temporary then
                temp_tubes = temp_tubes .. ', ' .. tube[1]
            end
        end

        if #temp_tubes ~= 0 then
            temp_tubes = temp_tubes:sub(3) -- Cut first ', '.
            opts['in_replicaset'] = false
            log.error('Queue: cannot set `in_replicaset = true`: '
                .. 'temporary tube(s) exists: ' .. temp_tubes)
        end
    end

    -- Check all options before configuring so that
    -- the configuration is done transactionally.
    for key, val in pairs(opts) do
        if key == 'ttr' then
            session_opts[key] = val
        elseif key == 'in_replicaset' then
            if type(val) ~= 'boolean' then
                error('Invalid value of in_replicaset: ' .. tostring(val))
            end
            session_opts[key] = val
        else
            error('Unknown option ' .. tostring(key))
        end
    end

    switch_in_replicaset(opts['in_replicaset'])

    -- Configuring the queue_session module.
    session.cfg(session_opts)

    for key, val in pairs(opts) do
        self[key] = val
    end
end

queue.cfg = setmetatable({}, { __call = cfg })

queue.statistics = function(space)
    if space ~= nil then
        return build_stats(space)
    end

    local stats = {}
    for _, tube_rc in box.space._queue:pairs() do
        local tube_name = tube_rc[1]
        stats[tube_name] = build_stats(tube_name)
    end

    return stats
end

queue.stats = queue.statistics

setmetatable(queue.stat, {
        __index = function(tbs, space)
            local spt = setmetatable({
                inc = function(t, cnt)
                    t[cnt] = t[cnt] + 1
                    return t[cnt]
                end,
                dec = function(t, cnt)
                    t[cnt] = t[cnt] - 1
                    return t[cnt]
                end
            }, {
                __index = function(t, cnt)
                    rawset(t, cnt, 0)
                    return 0
                end
            })
            rawset(tbs, space, spt)
            return spt
        end,
        __gc = function(tbs)
            for space, tubes in pairs(tbs) do
                for tube, tbt in pairs(tubes) do
                    rawset(tubes, tube, nil)
                end
                rawset(tbs, space, nil)
            end
        end
    }
)

return setmetatable(queue, { __index = method })
