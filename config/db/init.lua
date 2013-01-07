-- Global parameters
local PUSH_CHANNEL_SIZE     = 20
local PUSH_POOL_FIBERS      = 20

-- tuple structure
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

-- task statuses
local ST_READY      = 'r'
local ST_DELAYED    = 'd'
local ST_RUN        = '!'
local ST_BURIED     = 'b'
local ST_DONE       = '*'


if queue == nil then
    queue = {}
    queue.push_fiber_count = 0
    queue.push_channel = box.ipc.channel(PUSH_CHANNEL_SIZE)
    queue.consumers = {}

    setmetatable(queue.consumers, {
            __index = function(tbs, space)
                local spt = {}
                setmetatable(spt, {
                    __index = function(tbt, tube)
                        local channel = box.ipc.channel()
                        rawset(tbt, tube, channel)
                        return channel
                    end
                })
                rawset(tbs, space, spt)
                return spt
            end
        }
    )
end

queue.default = {}
    queue.default.pri = 0x7FFF
    queue.default.ttl = 3600 * 24 * 1
    queue.default.ttr = 60
    queue.default.delay = 0


queue.stat = {}
    queue.stat.ttl = 0
    queue.stat.ttr = 0
    queue.stat.done_ttl = 0
    queue.stat.race_cond = 0
    queue.stat.put = 0
    queue.stat.take = 0
    queue.stat.take_timeout = 0


local function wakeup_pushers()
    box.fiber.detach()
    box.fiber.yield()
    while(true) do
        box.fiber.sleep(.3)
        if queue.push_channel:has_readers() then
            queue.push_channel:put(nil)
        end
    end
end


local function process_tube(space, tube)
    while true do
        local now = os.time()
        local task = box.select_range(space, 1, 1, tube, ST_DELAYED)
        if task == nil then
            break
        end
        if task[i_status] ~= ST_DELAYED then
            break
        end
        local event = box.unpack('i', task[i_event])
        if event >= now then
            break
        end
        
        local ttl       = box.unpack('i', task[i_ttl])
        local started   = box.unpack('i', task[i_started])
       
        box.update(space, task[i_uuid],
            '=p=p',
                i_status,
                ST_READY,
                i_event,
                box.pack('i', started + ttl)
        )
    end
    
    while true do
        local task = box.select_range(space, 1, 1, tube, ST_RUN)
        if task == nil then
            break
        end
        if task[i_status] ~= ST_RUN then
            break
        end

        local now = os.time()
        local event     = box.unpack('i', task[i_event])
        if event >= now then
            break
        end

        local ttl       = box.unpack('i', task[i_ttl])
        local started   = box.unpack('i', task[i_started])
        
        if started + ttl >= now then
            box.delete(space, task[i_uuid])
            queue.stat.ttl = queue.stat.ttl + 1
        else
            box.update(space, task[i_uuid],
                '=p=p',
                i_status,
                ST_READY,
                i_event,
                box.pack('i', started + ttl)
            )

            queue.stat.ttr = queue.stat.ttr + 1
        end
    end

    while true do
        local task = box.select_range(space, 1, 1, tube, ST_DONE)
        if task == nil then
            break
        end
        if task[i_status] ~= ST_DONE then
            break
        end
        local now = os.time()
        local event     = box.unpack('i', task[i_event])
        if event >= now then
            break
        end
        queue.stat.ttl = queue.stat.ttl + 1
        queue.stat.done_ttl = queue.stat.done_ttl + 1
        box.delete(space, task[i_uuid])
    end
    
    while true do
        local task = box.select_range(space, 1, 1, tube, ST_READY)
        if task == nil then
            break
        end
        if task[i_status] ~= ST_READY then
            break
        end
        local now = os.time()
        local event     = box.unpack('i', task[i_event])
        if event < now then
            queue.stat.ttl = queue.stat.ttl + 1
            box.delete(space, task[i_uuid])
        else
            if not queue.consumers[space][tube]:has_readers() then
                break
            end
            local ttr = box.unpack('i', task[i_ttr])
            local ttl = box.unpack('i', task[i_ttl])
            local started = box.unpack('i', task[i_started])
            if started + ttl > now + ttr then
                event = now + ttr
            else
                event = started + ttl
            end
            task = box.update(space,
                task[i_uuid],
                '=p=p',
                i_status,
                ST_RUN,
                i_event,
                box.pack('i', event)
            )

            if queue.consumers[space][tube]:has_readers() then
                queue.consumers[space][tube]:put(task)
            else
                queue.stat.race_cond = queue.stat.race_cond + 1

                box.update(space,
                    task[i_uuid],
                    '=p=p',
                    i_status,
                    ST_READY,
                    i_event,
                    box.pack('i', started + ttl)
                )
            end
        end
    end
end

local function push_fiber()
    box.fiber.detach()
    box.fiber.yield()
    while(true) do
        local t = queue.push_channel:get()
        if t ~= nil then
            local space = t[1]
            local tube = t[2][ i_tube + 1 ]
            box.insert(space, unpack(t[2]))
        end

        for space, tubes in pairs(queue.consumers) do
            for tube, channel in pairs(tubes) do
                process_tube(space, tube)
            end
        end
    end
end


queue.statistic = function()
    return {
        'race_cond',
        tostring(queue.stat.race_cond),
        'ttl',
        tostring(queue.stat.ttl),
        'ttr',
        tostring(queue.stat.ttr),
        'done_ttl',
        tostring(queue.stat.done_ttl),
        'put',
        tostring(queue.stat.put),
        'take',
        tostring(queue.stat.take),
        'take_timeout',
        tostring(queue.stat.take_timeout),
    }
end

local function rettask(task)
    if task == nil then
        return
    end
    -- TODO: use tuple:transform here
    local tuple = {}
    if type(task) == 'table' then
        table.insert(tuple, task[i_uuid + 1])
        for i = i_task + 1, #task do
            table.insert(tuple, task[i])
        end
    else
        table.insert(tuple, task[i_uuid])
        for i = i_task, #task - 1 do
            table.insert(tuple, task[i])
        end
    end
    return tuple
end


queue.put = function(space, tube, ...)
-- `delay, ttl, ttr, pri, ...)

    local delay, ttl, ttr, pri, utask

    delay   = queue.default.delay
    ttl     = queue.default.ttl
    ttr     = queue.default.ttr
    pri     = queue.default.pri
    
    local arlen = select('#', ...)
    if arlen == 0 then          -- empty task
        utask    = {}
    elseif arlen == 1 then      -- put(task)
        utask    = {...}
    elseif arlen == 2 then      -- put(delay, task)
        delay = select(1, ...)
        utask  = { select(2, ...) }
    elseif arlen == 3 then      -- put(delay, ttl, task)
        delay = select(1, ...)
        ttl  = select(2, ...)
        utask = { select(3, ...) }
    elseif arlen == 4 then      -- put(delay, ttl, ttr, task)
        delay = select(1, ...)
        ttl  = select(2, ...)
        ttr  = select(3, ...)
        utask = { select(4, ...) }
    elseif arlen >= 5 then      -- full version of put
        delay = select(1, ...)
        ttl  = select(2, ...)
        ttr  = select(3, ...)
        pri  = select(4, ...)
        utask = { select(5, ...) }
    end
        

    delay = tonumber(delay)
    ttl = tonumber(ttl)
    ttr = tonumber(ttr)
    pri = tonumber(pri)

    if delay == 0 then
        delay = queue.default.delay
    end
    if ttl == 0 then
        ttl = queue.default.ttl
    end
    if ttr == 0 then
        ttr = queue.default.ttr
    end
    if pri == 0 then
        pri = queue.default.pri
    end

    local task = {}
    local now = os.time()
    task[ i_uuid + 1 ] = box.uuid_hex()
    task[ i_tube + 1 ] = tube
    if delay > 0 then
        ttl = ttl + delay
        task[ i_status + 1 ]    = ST_DELAYED
        task[ i_event + 1 ]     = box.pack('i', now + delay)
    else
        task[ i_status + 1 ]    = ST_READY
        task[ i_event + 1 ]     = box.pack('i', now + ttl)
    end
    task[ i_pri + 1 ]           = box.pack('i', pri)
    task[ i_cid + 1 ]           = ''
    task[ i_started + 1 ]       = box.pack('i', now)
    task[ i_ttl + 1 ]           = box.pack('i', ttl)
    task[ i_ttr + 1 ]           = box.pack('i', ttr)
    for i = 1, #utask do
        table.insert(task, utask[i])
    end

    if queue.push_fiber_count < PUSH_POOL_FIBERS then
        queue.push_fiber_count = queue.push_fiber_count + 1
        local fiber = box.fiber.create(push_fiber)
        box.fiber.resume(fiber)
        if queue.wakeup_fiber == nil then
            queue.wakeup_fiber = box.fiber.create(wakeup_pushers)
            box.fiber.resume(queue.wakeup_fiber)
        end
    end

    queue.push_channel:put({space, task})
    queue.stat.put = queue.stat.put + 1

    return rettask(task)
end

local function take_from_channel(space, tube, timeout)
    local task
    if timeout ~= nil then
        timeout = tonumber(timeout)
        if timeout > 0 then
            task = queue.consumers[space][tube]:get(timeout)
        else
            task = queue.consumers[space][tube]:get()
        end
    else
        task = queue.consumers[space][tube]:get()
    end
    queue.stat.take = queue.stat.take + 1
    if task == nil then
        queue.stat.take_timeout = queue.stat.take_timeout + 1
        return
    else
        return rettask(task)
    end
end

queue.take = function(space, tube, timeout)
    if queue.consumers[space][tube]:has_readers() then
        return take_from_channel(space, tube, timeout)
    end

    local task = box.select_range(space, 1, 1, tube, ST_READY)
    if task == nil then
        return take_from_channel(space, tube, timeout)
    end
    if task[i_status] ~= ST_READY then
        return take_from_channel(space, tube, timeout)
    end
    local now = os.time()
    local started = box.unpack('i', task[i_started])
    local ttr = box.unpack('i', task[i_ttl])
    local ttl = box.unpack('i', task[i_ttr])
    local event;

    if started + ttl > now + ttr then
        event = now + ttr
    else
        event = started + ttl
    end
    task = box.update(space,
        task[i_uuid],
            '=p=p',
            i_status,
            ST_RUN,
            i_event,
            box.pack('i', event)
    )

    queue.stat.take = queue.stat.take + 1
    return rettask(task)
end

queue.delete = function(space, task)
    local tuple = box.delete(space, task)
    if tuple ~= nil then
        return rettask(tuple)
    end
end

queue.ack = function(space, task)
    return queue.delete(space, task)
end

queue.done = function(space, task, ...)
    local tuple = box.select(space, 0, task)
    if tuple == nil then
        return
    end
    tuple = tuple:transform(i_task, #tuple, ...)
    tuple = tuple:transform(i_status, 1, ST_DONE)
    tuple = box.replace(space, tuple:unpack())
    return rettask(tuple)
end

queue.release = function(space, task, delay, pri, ttr, ttl)
    local tuple = box.select(space, 0, task)
    if tuple == nil then
        return
    end
    if tuple[i_status] ~= ST_RUN then
        return rettask(tuple)
    end

    local now = os.time()

    if ttl == nil then
        ttl = box.unpack('i', tuple[i_ttl])
    else
        ttl = tonumber(ttl)
    end

    if ttr == nil then
        ttr = box.unpack('i', tuple[i_ttr])
    else
        ttr = tonumber(ttr)
    end

    if delay == nil then
        delay = 0
    else
        delay = tonumber(delay)
        ttl = ttl + delay 
    end

    if pri == nil then
        pri = box.unpack('i', tuple[i_pri])
    else
        pri = tonumber(pri)
    end

    if delay > 0 then
        tuple = box.update(space,
            task,
            '=p=p=p=p=p',
            i_status,
            ST_DELAYED,
            i_event,
            now + delay,
            i_ttl,
            ttl,
            i_ttr,
            ttr,
            i_pri,
            pri
        )
    else
        tuple = box.update(space,
            task,
            '=p=p=p=p=p',
            i_status,
            ST_READY,
            i_event,
            box.unpack('i', tuple[i_started]) + ttl,
            i_ttl,
            ttl,
            i_ttr,
            ttr,
            i_pri,
            pri
        )
    end

    return rettask(tuple)
end

queue.task_status = function(space, task)
    local tuple = box.select(space, 0, task)
    if tuple == nil then
        return 'not found'
    end

    if tuple[i_status] == ST_READY then
        return 'ready'
    end
    if tuple[i_status] == ST_DELAYED then
        return 'delayed'
    end
    if tuple[i_status] == ST_RUN then
        return 'run'
    end
    if tuple[i_status] == ST_BURIED then
        return 'bury'
    end
    if tuple[i_status] == ST_DONE then
        return 'done'
    end
    return 'unknown: ' .. tuple[i_status]
end

queue.get = function(space, task)
    local tuple = box.select(space, 0, task)
    return rettask(tuple)
end
