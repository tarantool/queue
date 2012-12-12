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
    
    queue.stat = {}
    queue.stat.ttl = 0
    queue.stat.ttr = 0
    queue.stat.done_ttl = 0
    queue.stat.race_cond = 0
    queue.stat.put = 0
    queue.stat.take = 0
    queue.stat.take_timeout = 0
end


local function consumers_create_channels(space, tube)
    if queue.consumers[space] == nil then
        queue.consumers[space] = {}
        queue.consumers[space][tube] = box.ipc.channel(1)
    elseif queue.consumers[space][tube] == nil then
        queue.consumers[space][tube] = box.ipc.channel(1)
    end
end


local function process_tube(space, tube)
    while true do
        local now = os.time()
        local task = box.select_range(space, 1, 1, tube, ST_DELAYED)
        if task == nil then
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
                task[i_status],
                i_event,
                box.pack('i', started + ttl)
        )
    end
    
    while true do
        local task = box.select_range(space, 1, 1, tube, ST_RUN)
        if task == nil then
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
        local task = box.select_range(space, 1, 1, tube, ST_DONE)
        if task == nil then
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
            local ttr = task[i_ttr]
            local ttl = task[i_ttl]
            local started = task[i_started]
            if ttl > ttr then
                event = started + ttr
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
        local space = t[1]
        local tube = t[2][ i_tube + 1 ]
        box.insert(space, unpack(t[2]))
        consumers_create_channels(space, tube)

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

queue.put = function(space, tube, delay, ttl, ttr, pri, ...)
    delay = tonumber(delay)
    ttl = tonumber(ttl)
    ttr = tonumber(ttr)
    pri = tonumber(pri)

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
    for i = 1, select('#', ...) do
        local arg = select(i, ...)
        table.insert(task, arg)
    end

    if queue.push_fiber_count < PUSH_POOL_FIBERS then
        queue.push_fiber_count = queue.push_fiber_count + 1
        local fiber = box.fiber.create(push_fiber)
        box.fiber.resume(fiber)
    end

    queue.push_channel:put({space, task})
    queue.stat.put = queue.stat.put + 1

    return task
end

local function rettask(task)
    -- TODO: use tuple:transform here
    local tuple = { task[i_uuid], tostring(box.unpack('i', task[i_ttr])) }
    for i = i_task, #task - 1 do
        table.insert(tuple, task[i])
    end
    return tuple
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
    consumers_create_channels(space, tube)
    if queue.consumers[space][tube]:has_readers() then
        return take_from_channel(space, tube, timeout)
    end

    local task = box.select_range(space, 1, 1, tube, ST_READY)
    if task == nil then
        return take_from_channel(space, tube, timeout)
    end
    local started = box.unpack('i', task[i_started])
    local now = os.time()
    local ttr = box.unpack('i', task[i_ttl])
    local ttl = box.unpack('i', task[i_ttr])
    local event;

    if started + ttl > now + ttr then
        event = now + ttr
    else
        event = started + ttl
    end
    box.update(space,
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
