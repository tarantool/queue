local log      = require('log')
local fiber    = require('fiber')
local uuid     = require('uuid')

local session  = require('queue.abstract.queue_session')
local state    = require('queue.abstract.state')

local util     = require('queue.util')
local qc       = require('queue.compat')
local num_type = qc.num_type
local str_type = qc.str_type

local clock = require('clock')

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

local function tube_release_all_tasks(tube)
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
        tube.raw:release(task[1], {})
        released = released + 1
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

function tube.put(self, data, opts)
    local start = clock.proc64()

    opts = opts or {}
    local task = self.raw:put(data, opts)
    task = self.raw:normalize_task(task)

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.put_avg:insert(res)

    return task
end

local conds = {}
local releasing_connections = {}

function tube.take(self, timeout)
    local start = clock.proc64()

    timeout = util.time(timeout or util.TIMEOUT_INFINITY)
    local task = self.raw:take()
    if task ~= nil then
        task = self.raw:normalize_task(task)

        local res = (clock.proc64() - start) / 10^3
        queue.stat[self.name].extend.take_avg:insert(res)

        return task
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

        queue.stat[self.name].extend.conds_size = table.getn(conds)

        box.space._queue_consumers:delete{conn_id, fid}

        -- We don't take a task if the connection is in a
        -- disconnecting state.
        if releasing_connections[fid] then
            releasing_connections[fid] = nil

            queue.stat[self.name].extend.take_releasing_connections =
            queue.stat[self.name].extend.take_releasing_connections + 1
            local res = (clock.proc64() - start) / 10^3
            queue.stat[self.name].extend.take_avg_fail:insert(res)

            return nil
        end

        task = self.raw:take()

        if task ~= nil then
            task = self.raw:normalize_task(task)

            local res = (clock.proc64() - start) / 10^3
            queue.stat[self.name].extend.take_avg:insert(res)

            return task
        end

        local elapsed = fiber.time64() - started
        timeout = timeout > elapsed and timeout - elapsed or 0
    end

    queue.stat[self.name].extend.take_fail =
    queue.stat[self.name].extend.take_fail + 1
    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.take_avg_fail:insert(res)
end

function tube.touch(self, id, delta)
    local start = clock.proc64()

    if delta == nil then
        local res = (clock.proc64() - start) / 10^3
        queue.stat[self.name].extend.touch_avg:insert(res)
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
        local res = (clock.proc64() - start) / 10^3
        queue.stat[self.name].extend.touch_avg:insert(res)
        return
    end

    check_task_is_taken(self.tube_id, id)

    local space_name = box.space._queue:get{self.name}[3]
    queue.stat[space_name]:inc('touch')

    local task = self.raw:normalize_task(self.raw:touch(id, delta))

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.touch_avg:insert(res)

    return task
end

function tube.ack(self, id)
    local start = clock.proc64()

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

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.ack_avg:insert(res)

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
    local start = clock.proc64()

    local result = tube_release_internal(self, id, opts)

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.release_avg:insert(res)

    return result
end

function tube.peek(self, id)
    local start = clock.proc64()

    local task = self.raw:peek(id)
    if task == nil then
        error(("Task %s not found"):format(tostring(id)))
    end

    task = self.raw:normalize_task(task)

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.peek_avg:insert(res)

    return task
end

function tube.bury(self, id)
    local start = clock.proc64()

    local task = self:peek(id)
    local is_taken, _ = pcall(check_task_is_taken, self.tube_id, id)
    if is_taken then
        box.space._queue_taken_2.index.task:delete{self.tube_id, id}
    end
    if task[2] == state.BURIED then

        local res = (clock.proc64() - start) / 10^3
        queue.stat[self.name].extend.bury_avg:insert(res)

        return task
    end

    task = self.raw:normalize_task(self.raw:bury(id))

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.bury_avg:insert(res)

    return task
end

function tube.kick(self, count)
    local start = clock.proc64()

    count = count or 1

    local result = self.raw:kick(count)

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.kick_avg:insert(res)

    return result
end

function tube.delete(self, id)
    local start = clock.proc64()

    self:peek(id)

    local result = self.raw:normalize_task(self.raw:delete(id))

    local res = (clock.proc64() - start) / 10^3
    queue.stat[self.name].extend.delete_avg:insert(res)

    return result
end

-- drop tube
function tube.drop(self)
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
    self.raw:truncate()
end

function tube.on_task_change(self, cb)
    local old_cb = self.on_task_change_cb
    self.on_task_change_cb = cb or (function() end)
    return old_cb
end

function tube.grant(self, user, args)
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
        local prefix = (args.prefix or 'queue.tube') .. ('.%s:'):format(self.name)
        tube_grant_func(user, prefix .. 'take')
        tube_grant_func(user, prefix .. 'touch')
        tube_grant_func(user, prefix .. 'ack')
        tube_grant_func(user, prefix .. 'release')
        tube_grant_func(user, prefix .. 'peek')
        tube_grant_func(user, prefix .. 'bury')
        tube_grant_func(user, prefix .. 'kick')
        tube_grant_func(user, prefix .. 'delete')
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

        local queue_consumers = box.space._queue_consumers
        local queue_taken     = box.space._queue_taken_2

        -- if task was taken and become other state
        local taken = queue_taken.index.task:get{tube_id, task[1]}
        if taken ~= nil then
            queue_taken:delete{taken[1], taken[2]}
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
            queue_taken:insert{
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

            queue.stat[tube[1]]['extend']['release_on_disconnect'] =
            queue.stat[tube[1]]['extend']['release_on_disconnect'] + 1
        end
    end
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
    opts = opts or {}
    if opts.if_not_exists == nil then
        opts.if_not_exists = false
    end
    if opts.if_not_exists == true and queue.tube[tube_name] ~= nil then
        return queue.tube[tube_name]
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

    local _taken = box.space._queue_taken_2
    if _taken == nil then
        -- tube_id, task_id, connection_id, session_uuid, time
        _taken = box.schema.create_space('_queue_taken_2', {
            temporary = true,
            format = {
                {name = 'tube_id', type = num_type()},
                {name = 'task_id', type = num_type()},
                {name = 'connection_id', type = num_type()},
                {name = 'session_uuid', type = str_type()},
                {name = 'taken_time', type = num_type()}
            }})

        _taken:create_index('task', {
            type = 'tree',
            parts = {1, num_type(), 2, num_type()},
            unique = true
        })
        _taken:create_index('uuid', {
            type = 'tree',
            parts = {4, str_type()},
            unique = false
        })
    end

    for _, tube_tuple in _queue:pairs() do
        -- Recreate tubes for registered drivers only.
        -- Check if a driver exists for this type of tube.
        if queue.driver[tube_tuple[4]] ~= nil then
            local tube = recreate_tube(tube_tuple)
            -- gh-66: release all taken tasks on start
            tube_release_all_tasks(tube)
        end
    end

    session.on_session_remove(release_session_tasks)
    session.start()

    connection.on_disconnect(queue._on_consumer_disconnect)
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
            tube_release_all_tasks(tube)
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
    }, extend = {}}

    local st = rawget(queue.stat, space) or {}
    local idx_tube = 1

    -- add api calls stats
    local calls_total = 0
    for name, value in pairs(st) do
        if type(value) ~= 'function' and name ~= 'done' and name ~= 'extend' then
            stats['calls'][name] = value
            calls_total = calls_total + value
        end
    end
    stats['calls']['total'] = calls_total

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

    for name, value in pairs(st.extend) do
        if string.find(name, '_avg') then
            stats['extend'][name] = st['extend'][name]:get_avarage()
        else
            stats['extend'][name] = value
        end
    end

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

    -- Check all options before configuring so that
    -- the configuration is done transactionally.
    for key, val in pairs(opts) do
        if key == 'ttr' then
            session_opts[key] = val
        else
            error('Unknown option ' .. tostring(key))
        end
    end

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
local sample_size = 1000

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
                end,
                extend = {
                    release_on_disconnect = 0,
                    take_releasing_connections = 0,
                    take_fail = 0,
                    conds_size = 0,
                    put_avg = util.moving_avarage_calc_new(sample_size),
                    take_avg = util.moving_avarage_calc_new(sample_size),
                    take_avg_fail = util.moving_avarage_calc_new(sample_size),
                    touch_avg = util.moving_avarage_calc_new(sample_size),
                    ack_avg = util.moving_avarage_calc_new(sample_size),
                    release_avg = util.moving_avarage_calc_new(sample_size),
                    peek_avg = util.moving_avarage_calc_new(sample_size),
                    bury_avg = util.moving_avarage_calc_new(sample_size),
                    kick_avg = util.moving_avarage_calc_new(sample_size),
                    delete_avg = util.moving_avarage_calc_new(sample_size),
                }
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
