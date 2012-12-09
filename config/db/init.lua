if queue == nil then
    queue = {}
    queue.service = {}
    queue.tube = {}
    queue.default = {}
    queue.service.channel = box.ipc.channel(1)
    queue.service.workers = {}


    -- default parameters
    queue.default.delay = 0
    queue.default.pri = 2 ^ 26
    queue.default.ttl = 86400 * 365 -- 1 year
    queue.default.ttr = 10 * 60 -- 10 minute

--     queue.service = box.ipc.channel(1)
end

local i_uuid        = 0
local i_tube        = 1
local i_status      = 2
local i_event       = 3
local i_pri         = 4
local i_cid         = 5
local i_started     = 6
local i_ttl         = 7
local i_ttr         = 8
local i_task        = 9


local function cp_channel(space, tube)
    if queue.tube[tube] == nil then
        queue.tube[tube] = {}
        queue.tube[tube][space] = box.ipc.channel(1)
        return
    end
    if queue.tube[tube][space] == nil then
        queue.tube[tube][space] = box.ipc.channel(1)
    end
end

-- queue.put
--   1. queue.put(sno, tube, task)
--      - put task into tube queue
--   2. queue.put(sno, tube, delay, task)
--      - put task into tube queue with delay
--   3. queue.put(sno, tube, delay, ttl, task)
--      - put task into tube queue with delay and ttl options
--   4. queue.put(sno, tube, delay, ttl, ttr, pri, task, ...)
--      - put task into tube queue with additional arguments and all options
queue.put = function(space, tube, ...)
    local pri = queue.default.pri
    local ttl = queue.default.ttl
    local ttr = queue.default.ttr
    local delay = queue.default.delay
    local task

    cp_channel(space, tube)

    local acount = select('#', ...)
    if acount == 0 then
        task = { }
    elseif acount == 1 then                 -- put(sno, tube, task)
        task = { ... }
    elseif acount == 2 then                 -- put(sno, tube, delay, task)
        delay = tonumber(select(1, ...))
        task = { select(2, ...) }
    elseif acount == 3 then                 -- put(sno, tube, delay, ttl, task)
        delay = tonumber(select(1, ...))
        ttl = tonumber(select(2, ...))
        task = { select(3, ...) }
    else
        delay = tonumber(select(1, ...))
        ttl   = tonumber(select(2, ...))
        ttr   = tonumber(select(3, ...))
        pri   = queue.default.pri + tonumber(select(4, ...))
        task  = tonumber(select(5, ...))
    end

    if ttl == 0 then
        ttl = queue.default.ttl
    end
    if ttr == 0 then
        ttl = queue.default.ttr
    end

    local now = os.time()
    local event
    local status

    if delay > 0 then
        ttl = ttl + delay
        status = 'delayed'
        event = now + ttl
    else
        status = 'ready'
        event = now
    end


    local tuple = { }
    tuple[ i_uuid + 1    ] = box.uuid_hex()
    tuple[ i_tube + 1    ] = tube
    tuple[ i_status + 1  ] = status
    tuple[ i_event + 1   ] = box.pack('i', event)
    tuple[ i_pri + 1     ] = box.pack('i', pri)
    tuple[ i_cid + 1     ] = ''
    tuple[ i_started + 1 ] = box.pack('i', now)
    tuple[ i_ttl + 1     ] = box.pack('i', ttl)
    tuple[ i_ttr + 1     ] = box.pack('i', ttr)
    for i, a in pairs(task) do
        table.insert(tuple, a)
    end

    queue.service.wakeup(space, tube)
    task = box.insert(space, unpack(tuple))
    return task
end

-- task = queue.take(space, tube)
-- task = queue.take(space, tube, timeout)
queue.take = function(space, tube, timeout)
    if timeout == nil then
        timeout = 0
    else
        timeout = tonumber(timeout)
    end

    cp_channel(space, tube)
    if queue.tube[tube][space]:has_readers() then
        return queue.tube[tube][space]:get(timeout)
    end
    
    -- select ready
    local task = box.select_range(space, 1, 1, tube, 'ready')
    if task ~= nil then
        local now = os.time()
        local event = now + task[i_ttr]
        local tl = task[i_started] + task[i_ttl]
        if tl < event then
            event = tl
        end
        if task[i_event] < now then
            return box.update(space, task[i_uuid], '=p=p',
                i_event,
                event,
                i_status,
                'run'
            )
        else
            queue.service.wakeup()
        end
    end
    return queue.tube[tube][space]:get(timeout)
end

queue.service.process_tube = function(space, tube)

    -- run -> ready | done
    while true do
        local task = box.select_range(space, 1, 1, tube, 'run')
        if task == nil then
            break
        end
        local now = os.time()
        if task[i_event] > now then
            break
        end
        if task[i_started] + task[i_ttl] <= now then
            box.update(space, task[i_uuid], '=p=p',
                i_status, 'ready',
                i_event, task[i_started], task[i_ttl]
            )
        else
            box.delete(space, task[i_uuid])
        end
    end

    -- delay -> ready
    while true do
        local task = box.select_range(space, 1, 1, tube, 'delay')
        if task == nil then
            break
        end
        local now = os.time()
        if task[i_event] > now then
            break
        end
        box.update(space, task[i_uuid], '=p=p',
            i_status, 'ready',
            i_event,  task[i_started] + task[i_ttl]
        )
    end

    -- select ready
    while true do
        local task = box.select_range(space, 1, 1, tube, 'ready')
        if task == nil then
            return
        end

        local now = os.time()
        if task[i_event] < now then
            if queue.tube[tube][space]:has_readers() then
                queue.tube[tube][space]:put(task)
            end
            return
        end
        box.delete(space, tash[i_uuid])
    end
end


queue.service.worker = function()
    box.fiber.detach()
    box.fiber.yield()
    while(true) do
        for i, space in pairs(queue.tube) do
            for tube, channel in pairs(space) do
                queue.service.process_tube(space, tube)
            end
        end
        local taskp = queue.service.channel:get(0.5)
        if taskp ~= nil then
            queue.service.process_tube(unpack(taskp))
        end
    end
end

queue.service.wakeup = function(space, tube)
    for i = 1, 10 do
        local fiber = box.fiber.create(queue.service.worker)
        table.insert(queue.service.workers, fiber)
        box.fiber.resume(fiber)
    end

    queue.service.wakeup = function(space, tube)
        if queue.service.channel:has_readers() then
            queue.service.channel:put({space, tube })
        end
    end
    queue.service.wakeup(space, tube)
end


