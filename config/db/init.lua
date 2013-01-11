-- queue in tarantool

-- tarantool config example:
-- space = [
--     {
--         enabled = 1,
--         index = [
--             {
--                 type = "TREE",
--                 unique = 1,
--                 key_field = [
--                     {
--                         fieldno = 0,
--                         type = "STR"
--                     }
--                 ]
--             },
--             {
--                 type = "TREE",
--                 unique = 0,
--                 key_field = [
--                     {
--                         fieldno = 1,    # tube
--                         type = "STR"
--                     },
--                     {
--                         fieldno = 2,    # status
--                         type = "STR"
--                     },
--                     {
--                         fieldno = 3,    # next_event (timestamp)
--                         type = "NUM"
--                     },
--                     {
--                         fieldno = 4,    # pri
--                         type = "NUM"
--                     }
--                 ]
--             }
--         ]
--     }
-- ]

-- space - number of space contains tubes
-- tube - queue name
-- ttl - time to live
-- ttr - time to release (when task is run)
-- delay - delay period for task


local function to_time64(time)
    return time * 1000000
end

local function from_time64(time)
    return tonumber(time / 1000000.0)
end

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

-- indexes
local idx_task      = 0
local idx_tube      = 1
local idx_event     = 2

-- task statuses
local ST_READY      = 'r'
local ST_DELAYED    = 't'
local ST_RUN        = '!'
local ST_BURIED     = 'b'
local ST_DONE       = 'd'


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
    queue.stat.ttl = { total = 0, done = 0, ready = 0, run = 0 }
    queue.stat.ttr = 0
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

    local now = box.time64()
    while true do
        local task = box.select_range(space, idx_event, 1)

        if task == nil then
            break
        end

        if box.unpack('l', task[i_event]) > now then
            break
        end

        local ttl = box.unpack('l', task[i_ttl])
        local started = box.unpack('l', task[i_started])
        local ttr = box.unpack('l', task[i_ttr])


        if now >= started + ttl then
            queue.stat.ttl.total = queue.stat.ttl.total + 1
            -- TODO: fill all statuses
                

            box.delete(space, task[i_uuid])

        -- delayed -> ready
        elseif task[i_status] == ST_DELAYED then
            box.update(space, task[i_uuid],
                '=p=p',
                i_status,
                ST_READY,
                i_event,
                box.pack('l', started + ttl)
            )
        elseif task[i_status] == ST_RUN then
            box.update(space, task[i_uuid],
                '=p=p=p',
                i_status,
                ST_READY,
                i_cid,
                box.pack('i', 0),
                i_event,
                box.pack('l', started + ttl)
            )
        else
            print("Internal error event in status ", task[i_uuid])
            box.update(space, task[i_uuid],
                '=p',
                i_event,
                box.pack('l', now + 5000000)
            )
        end
    end

    
    while queue.consumers[space][tube]:has_readers() do
        
        local task = box.select_range(space, idx_tube, 1, tube, ST_READY)
        if task == nil then
            break
        end
        local ttl = box.unpack('l', task[i_ttl])
        local started = box.unpack('l', task[i_started])
        local ttr = box.unpack('l', task[i_ttr])

        local event = now + ttr
        if event > started + ttl then
            event = started + ttl
        end
    
        queue.consumers[space][tube]:put(task)
        box.fiber.yield()
    end

end

local function push_fiber()
    box.fiber.detach()
    box.fiber.yield()
    while(true) do
        local t = queue.push_channel:get()
        if t ~= nil then
            local space = t[1]
            local tube = t[2]
            process_tube(space, tube)
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
        tostring(queue.stat.ttl.total),
        'ttr',
        tostring(queue.stat.ttr),
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
        

    delay   = tonumber(delay)
    ttl     = tonumber(ttl)
    ttr     = tonumber(ttr)
    pri     = tonumber(pri)

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


    delay   = to_time64(delay)
    ttl     = to_time64(ttl)
    ttr     = to_time64(ttr)


    local task
    local now = box.time64()

    if delay > 0 then
        task = {
            box.uuid_hex(),
            tube,
            ST_DELAYED,
            box.pack('l', now + delay),
            box.pack('i', pri),
            box.pack('i', 0),
            box.pack('l', now),
            box.pack('l', ttl),
            box.pack('l', ttr),
        }
    else 
        task = {
            box.uuid_hex(),
            tube,
            ST_READY,
            box.pack('l', now + ttl),
            box.pack('i', pri),
            box.pack('i', 0),
            box.pack('l', now),
            box.pack('l', ttl),
            box.pack('l', ttr),
        }
    end

    for i = 1, #utask do
        table.insert(task, utask[i])
    end
    
    box.insert(space, unpack(task))

    if queue.push_fiber_count < PUSH_POOL_FIBERS then
        queue.push_fiber_count = queue.push_fiber_count + 1
        local fiber = box.fiber.create(push_fiber)
        box.fiber.resume(fiber)
        if queue.wakeup_fiber == nil then
            queue.wakeup_fiber = box.fiber.create(wakeup_pushers)
            box.fiber.resume(queue.wakeup_fiber)
        end
    end


    queue.push_channel:put({space, tube})
    queue.stat.put = queue.stat.put + 1

    return rettask(task)
end

local function take_from_channel(space, tube, timeout)
    local task

    if timeout == nil then
        timeout = 0
    else
        timeout = tonumber(timeout)
        if timeout < 0 then
            timeout = 0
        end
    end

    
    if timeout > 0 then
        local started = from_time64(box.time64())
        while true do
            local now = from_time64(box.time64())
            if now >= started + timeout then
                break
            end
            task = queue.consumers[space][tube]:get(timeout - (now - started))
            if task == nil then
                queue.stat.take_timeout = queue.stat.take_timeout + 1
                return
            end
            task = box.select(space, idx_task, task[i_uuid])
            if task ~= nil and task[i_status] == ST_READY then
                return task
            end
            queue.stat.race_cond = queue.stat.race_cond + 1
        end
    else
        while true do
            task = queue.consumers[space][tube]:get()
            task = box.select(space, idx_task, task[i_uuid])
            if task ~= nil and task[i_status] == ST_READY then
                return task
            end
            queue.stat.race_cond = queue.stat.race_cond + 1
        end
    end
end

queue.take = function(space, tube, timeout)

    local task

    if queue.consumers[space][tube]:has_readers() then
        task = take_from_channel(space, tube, timeout)
    else
        task = box.select_range(space, idx_tube, 1, tube, ST_READY)
        if task == nil or task[i_status] ~= ST_READY then
            task = take_from_channel(space, tube, timeout)
        end
    end

    if task == nil then
        return
    end

    local now = box.time64()
    local started = box.unpack('l', task[i_started])
    local ttr = box.unpack('l', task[i_ttl])
    local ttl = box.unpack('l', task[i_ttr])
    local event = now + ttr
    if event > started + ttl then
        event = started + ttl
    end

    task = box.update(space,
        task[i_uuid],
            '=p=p=p',
            i_status,
            ST_RUN,

            i_event,
            box.pack('l', event),

            i_cid,
            box.session.id()
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
    local tuple = box.select(space, idx_task, task)
    if tuple == nil then
        return
    end
    if tuple[i_status] ~= ST_RUN then
        return rettask(tuple)
    end

    local now = box.time64()

    if ttl == nil then
        ttl = box.unpack('l', tuple[i_ttl])
    else
        ttl = to_time64(tonumber(ttl))
    end

    if ttr == nil then
        ttr = box.unpack('l', tuple[i_ttr])
    else
        ttr = to_time64(tonumber(ttr))
    end

    if delay == nil then
        delay = 0
    else
        delay = to_time64(tonumber(delay))
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
            '=p=p=p=p=p=p',
            i_status,
            ST_DELAYED,
            i_event,
            now + delay,
            i_ttl,
            ttl,
            i_ttr,
            ttr,
            i_pri,
            pri,
            i_cid,
            0
        )
    else
        tuple = box.update(space,
            task,
            '=p=p=p=p=p=p',
            i_status,
            ST_READY,
            i_event,
            box.unpack('l', tuple[i_started]) + ttl,
            i_ttl,
            ttl,
            i_ttr,
            ttr,
            i_pri,
            pri,
            i_cid,
            0
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
