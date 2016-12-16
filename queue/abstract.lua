local log      = require('log')
local fun      = require('fun')
local fiber    = require('fiber')

local state    = require('queue.abstract.state')
local num_type = require('queue.compat').num_type
local str_type = require('queue.compat').str_type

local session = box.session

local queue = {
    tube = {},
    stat = {}
}
local TIMEOUT_INFINITY = 365 * 86400 * 1000

local function time(tm)
    tm = tm and tm * 1000000 or fiber.time64()
    return 0ULL + tm
end

local function event_time(timeout)
    return fiber.time64() + time(timeout)
end

-- load all drivers
queue.driver = {
    fifo        = require('queue.abstract.driver.fifo'),
    fifottl     = require('queue.abstract.driver.fifottl'),
    utube       = require('queue.abstract.driver.utube'),
    utubettl    = require('queue.abstract.driver.utubettl')
}

-- tube methods
local tube = {}

function tube.put(self, data, opts)
    opts = opts or {}
    local task = self.raw:put(data, opts)
    return self.raw:normalize_task(task)
end

function tube.take(self, timeout)
    timeout = time(timeout or TIMEOUT_INFINITY)
    local task = self.raw:take()
    if task ~= nil then
        return self.raw:normalize_task(task)
    end

    while timeout > 0 do
        local started = fiber.time64()
        local time = event_time(timeout)
        local tube_id = self.tube_id

        box.space._queue_consumers:insert{
                session.id(), fiber.id(), tube_id, time, fiber.time64() }
        fiber.sleep(tonumber(timeout) / 1000000)
        box.space._queue_consumers:delete{ session.id(), fiber.id() }

        task = self.raw:take()

        if task ~= nil then
            return self.raw:normalize_task(task)
        end

        local elapsed = fiber.time64() - started
        timeout = timeout > elapsed and timeout - elapsed or 0
    end
end

function tube.touch(self, id, increment)
    if increment < 0 then
        error("Increment can't be less than zero")
    end

    if increment == 0 then
        return
    end

    local task = self:peek(id)
    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken == nil then
        error("Task was not taken in the session")
    end

    queue.stat[space_name]:inc('touch')
    return self.raw:normalize_task(self.raw:touch(id, increment))
end

function tube.ack(self, id)
    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken == nil then
        error("Task was not taken in the session")
    end
    local tube = box.space._queue:get{self.name}
    local space_name = tube[3]

    self:peek(id)
    -- delete task
    box.space._queue_taken:delete{session.id(), self.tube_id, id}
    local result = self.raw:normalize_task(
        self.raw:delete(id):transform(2, 1, state.DONE)
    )
    -- swap delete and ack call counters
    queue.stat[space_name]:inc('ack')
    queue.stat[space_name]:dec('delete')
    return result
end

function tube.release(self, id, opts)
    opts = opts or {}
    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken == nil then
        error("Task was not taken in the session")
    end

    box.space._queue_taken:delete{session.id(), self.tube_id, id}
    self:peek(id)
    return self.raw:normalize_task(self.raw:release(id, opts))
end

function tube.peek(self, id)
    local task = self.raw:peek(id)
    if task == nil then
        error(("Task %s not found"):format(tostring(id)))
    end
    return self.raw:normalize_task(task)
end

function tube.bury(self, id)
    local task = self:peek(id)
    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken ~= nil then
        box.space._queue_taken:delete{session.id(), self.tube_id, id}
    end
    if task[2] == state.BURIED then
        return task
    end
    return self.raw:normalize_task(self.raw:bury(id))
end

function tube.kick(self, count)
    count = count or 1
    return self.raw:kick(count)
end

function tube.delete(self, id)
    self:peek(id)
    return self.raw:normalize_task(
        self.raw:delete(id):transform(2, 1, state.DONE))
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

    local taken = box.space._queue_taken.index.task:min{tube_id}
    if taken ~= nil and taken[2] == tube_id then
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

-- methods
local method = {}

local function make_self(driver, space, tube_name, tube_type, tube_id, opts)
    opts = opts or {}
    local self

    -- wakeup consumer if queue have new task
    local on_task_change = function(task, stats_data)
        self.on_task_change_cb(task, stats_data)

        -- task was removed
        if task == nil then return end

        local queue_consumers = box.space._queue_consumers
        local queue_taken     = box.space._queue_taken

        -- if task was taken and become other state
        local taken = queue_taken.index.task:get{tube_id, task[1]}
        if taken ~= nil then
            queue_taken:delete{taken[1], taken[2], taken[3]}
        end
        -- task swicthed to ready (or new task)
        if task[2] == state.READY then
            local tube_id = self.tube_id
            local consumer = queue_consumers.index.consumer:min{tube_id}

            if consumer ~= nil then
                if consumer[3] == tube_id then
                    fiber.find(consumer[2]):wakeup()
                    queue_consumers:delete{consumer[1], consumer[2]}
                end
            end
        -- task swicthed to taken - registry in taken space
        elseif task[2] == state.TAKEN then
            queue_taken:insert{session.id(), self.tube_id, task[1], fiber.time64()}
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
    self:on_task_change(opts.on_task_change)
    queue.tube[tube_name] = self

    return self
end

function method._on_consumer_disconnect()
    local waiter, fb, task, tube, id
    id = session.id()
    -- wakeup all waiters
    while true do
        waiter = box.space._queue_consumers.index.pk:min{id}
        if waiter == nil then
            break
        end
        -- Don't touch the other consumers
        if waiter[1] ~= id then
            break
        end
        box.space._queue_consumers:delete{ waiter[1], waiter[2] }
        fb = fiber.find(waiter[2])
        if fb ~= nil and fb:status() ~= 'dead' then
            fb:wakeup()
        end
    end

    -- release all session tasks
    while true do
        task = box.space._queue_taken.index.pk:min{id}
        if task == nil or task[1] ~= id then
            break
        end

        tube = box.space._queue.index.tube_id:get{task[2]}
        if tube == nil then
            log.error("Inconsistent queue state: tube %d not found", task[2])
            box.space._queue_taken:delete{task[1], task[2], task[3] }
        else
            log.warn("Consumer %s disconnected, release task %s(%s)",
                id, task[3], tube[1])

            queue.tube[tube[1]]:release(task[3])
        end
    end
end

-- function takes tuples and recreates tube
local function recreate_tube(tube_tuple)
    local name, id, space, tube_type, opts = tube_tuple:unpack()

    local driver = queue.driver[tube_type]
    if driver == nil then
        error("Unknown tube type " .. tostring(tube_type))
    end

    local space = box.space[space]
    if space == nil then
        error(("Space '%s' doesn't exists"):format(space))
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

    -- create tube record
    local last = box.space._queue.index.tube_id:max()
    local tube_id = 0
    if last ~= nil then
        tube_id = last[2] + 1
    end

    -- create tube space
    local space = driver.create_space(space_name, opts)
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
        _queue = box.schema.create_space('_queue')
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
        -- session, fid, tube, time
        _cons = box.schema.create_space('_queue_consumers', {temporary = true})
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

    local _taken = box.space._queue_taken
    if _taken == nil then
        -- session_id, tube_id, task_id, time
        _taken = box.schema.create_space('_queue_taken', {temporary = true})
        _taken:create_index('pk', {
            type = 'tree',
            parts = {1, num_type(), 2, num_type(), 3, num_type()},
            unique = true})

        _taken:create_index('task',{
            type = 'tree',
            parts = {2, num_type(), 3, num_type()},
            unique = true
        })
    end

    _queue:pairs():each(recreate_tube)

    session.on_disconnect(queue._on_consumer_disconnect)
    return queue
end

local function build_stats(space)
    local stats = {tasks = {}, calls = {
        ack = 0,
        bury = 0,
        delete = 0,
        kick = 0,
        put = 0,
        release = 0,
        take = 0,
        touch = 0
    }}

    local st = rawget(queue.stat, space) or {}
    local idx_tube = 1

    -- add api calls stats
    for name, value in pairs(st) do
        if type(value) ~= 'function' and name ~= 'done' then
            stats['calls'][name] = value
        end
    end

    -- add total tasks count
    stats['tasks']['total'] = box.space[space].index[idx_tube]:count()

    -- add tasks by state count
    for i, s in pairs(state) do
        stats['tasks'][i:lower()] = box.space[space].index[idx_tube]:count(s)
    end
    stats['tasks']['done'] = st.done or 0

    return stats
end

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

queue.register_driver = function(driver_name, tube_ctr)
    if type(tube_ctr.create_space) ~= 'function' or
       type(tube_ctr.new) ~= 'function' then
        error('tube control methods must contain functions "create_space" and "new"')
    end
    queue.driver[driver_name] = tube_ctr
end

return setmetatable(queue, { __index = method })
