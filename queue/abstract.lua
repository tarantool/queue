local fiber = require 'fiber'
local log = require 'log'
local session = box.session
local queue = { driver = {}, tube = {} }
local TIMEOUT_INFINITY  = 365 * 86400 * 1000

local function time()
    return tonumber64(fiber.time() * 1000000)
end

local function event_time(timeout)
    return tonumber64((fiber.time() + timeout) * 1000000)
end

-- load all drivers
queue.driver.fifo = require 'queue.abstract.driver.fifo'


-- tube methods
local tube = {}

function tube.take(self)
    local task = self.raw:take()
    if task ~= nil then
        return task
    end
end


function tube.put(self, data, opts)
    if opts == nil then
        opts = {}
    end
    local task = self.raw:put(data, opts)
    if task == nil then
        return nil
    end

    local tube_id = self.tube_id
    local consumer = box.space._queue_consumers.index.consumer:min{ tube_id }

    if consumer ~= nil then
        if consumer[3] == tube_id then 
            fiber.find( consumer[2] ):wakeup()
            box.space._queue_consumers:delete{ consumer[1], consumer[2] }
        end
    end

    return task
end

function register_taken(self, task)
    if task == nil then
        return
    end
    box.space._queue_taken:insert{session.id(), self.tube_id, task[1], fiber.time()}
    return task
end

function tube.take(self, timeout)
    if timeout == nil then
        timeout = TIMEOUT_INFINITY
    end
    local task = self.raw:take()
    if task ~= nil then
        return register_taken(self, task)
    end

    while timeout > 0 do
        local started = fiber.time()
        local time = event_time(timeout)
        local tube_id = self.tube_id
        
        box.space._queue_consumers:insert{
                session.id(), fiber.id(), tube_id, time, fiber.time() }
        fiber.sleep(timeout)
        box.space._queue_consumers:delete{ session.id(), fiber.id() }
        task = self.raw:take()

        if task ~= nil then
            return register_taken(self, task)
        end

        timeout = timeout - (fiber.time() - started)
    end
end

function tube.ack(self, id)
    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken == nil then
        box.error(box.error.PROC_LUA, "Task is not found")
    end

    box.space._queue_taken:delete{session.id(), self.tube_id, id}
    return self.raw:delete(id)
end


-- methods
local method = {}

-- create tube
function method.create_tube(tube_name, tube_type, opts)
    local driver = queue.driver[tube_type]
    if driver == nil then
        box.error(box.error.PROC_LUA, "Unknown tube type " .. tostring(tube_type))
    end
    local space_name = string.format('queue_%s_%s', tube_type, tube_name)
    if box.space[space_name] ~= nil then
        box.error(box.error.PROC_LUA, "Space " .. space_name .. " already exists")
    end
   
    -- create tube record
    local last = box.space._queue.index.tube_id:max()
    local tube_id = 0
    if last ~= nil then
        tube_id = last[2] + 1
    end
    box.space._queue:insert{ tube_name, tube_id, space_name, tube_type, opts }

    -- create tube space
    local space = driver.create_space(space_name, opts)

    -- bless tube object
    local self = {
        raw     = driver.new(space),
        name    = tube_name,
        type    = tube_type,
        tube_id = tube_id
    }
    setmetatable(self, {__index = tube})
    queue.tube[tube_name] = self
    return self
end


-- create or join infrastructure
function method.start()

    local _queue = box.space._queue
    if _queue == nil then
        _queue = box.schema.create_space('_queue')
        _queue:create_index('tube', { type = 'tree', parts = { 1, 'str' }})
        _queue:create_index('tube_id',
            { type = 'tree', parts = { 2, 'num' }, unique = true })
    end

    local _cons = box.space._queue_consumers
    if _cons == nil then
        -- session, fid, tube, time
        _cons = box.schema.create_space('_queue_consumers', { temporary = true })
        _cons:create_index('pk', { type = 'tree', parts = { 1, 'num', 2, 'num' }})
        _cons:create_index('consumer',
            { type = 'tree', parts = { 3, 'num', 4, 'num' }})
    end

    local _taken = box.space._queue_taken
        -- session_id, tube_id, task_id, time
    if _taken == nil then
        _taken = box.schema.create_space('_queue_taken', { temporary = true })
        _taken:create_index('pk',
            { type = 'tree', parts = { 1, 'num', 2, 'num', 3, 'num'}})
    end

    for _, tube_rc in _queue:pairs() do
        local tube_name     = tube_rc[1]
        local tube_id       = tube_rc[2]
        local tube_space    = tube_rc[3]
        local tube_type     = tube_rc[4]
        local tube_opts     = tube_rc[5]

        local driver = queue.driver[tube_type]
        if driver == nil then
            box.error(box.error.PROC_LUA,
                "Unknown tube type " .. tostring(tube_type))
        end


        local space = box.space[tube_space]
        if space == nil then
            box.error(box.error.PROC_LUA,
                "Space " .. tube_space .. " is not exists")
        end
        local self = {
            raw         = driver.new(space),
            name        = tube_name,
            type        = tube_type,
            tube_id     = tube_id
        }
        setmetatable(self, {__index = tube})
        queue.tube[tube_name] = self
    end

    return true
end


setmetatable(queue, { __index = method })
return queue
