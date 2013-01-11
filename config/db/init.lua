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
--                         fieldno = 4,    # pri
--                         type = "NUM"
--                     }
--                 ]
--             },
--             {
--                 type    = "TREE",
--                 unique  = 0,
--                 key_field = [
--                     {
--                         fieldno = 3,    # next_event
--                         type = "NUM64"
--                     }
--                 ]
--             }
--         ]
--     }
-- ]

-- Glossary
-- space - number of space contains tubes
-- tube - queue name
-- ttl - time to live
-- ttr - time to release (when task is run)
-- delay - delay period for task


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
local i_cready      = 9
local i_cbury       = 10
local i_task        = 11

-- indexes
local idx_task      = 0
local idx_tube      = 1
local idx_event     = 2

-- task statuses
local ST_READY      = 'r'
local ST_DELAYED    = 'd'
local ST_TAKEN      = 't'
local ST_BURIED     = 'b'
local ST_DONE       = '*'


local human_status = {}
    human_status[ST_READY]      = 'ready'
    human_status[ST_DELAYED]    = 'delayed'
    human_status[ST_TAKEN]      = 'taken'
    human_status[ST_BURIED]     = 'buried'
    human_status[ST_DONE]       = 'done'


local function to_time64(time)
    return time * 1000000
end

local function from_time64(time)
    return tonumber(time) / 1000000.0
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


local function process_tube(space, tube)

    while true do
        local now = box.time64()
        local next_event = 3600
        while true do
            local task = box.select_range(space, idx_event, 1)

            if task == nil then
                break
            end

            local event = box.unpack('l', task[i_event])
            if event > now then
                next_event = from_time64(event - now)
                break
            end

            local ttl = box.unpack('l', task[i_ttl])
            local started = box.unpack('l', task[i_started])
            local ttr = box.unpack('l', task[i_ttr])


            if now >= started + ttl then
                queue.stat.ttl.total = queue.stat.ttl.total + 1
                -- TODO: fill all statuses

                queue.stat.ttl[ human_status[task[i_status]] ] =
                    queue.stat.ttl[ human_status[task[i_status]] ] + 1

                
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
                queue.consumers[space][tube]:put(true)
            elseif task[i_status] == ST_TAKEN then
                box.update(space, task[i_uuid],
                    '=p=p=p',
                    i_status,
                    ST_READY,
                    i_cid,
                    box.pack('i', 0),
                    i_event,
                    box.pack('l', started + ttl)
                )
                queue.consumers[space][tube]:put(true)
            else
                box.update(space, task[i_uuid],
                    '=p',
                    i_event,
                    box.pack('l', now + to_time64(5))
                )
            end
            now = box.time64()
        end

        queue.workers[space][tube].ch:get(next_event)
--         box.fiber.sleep(next_event)
    end
end



if queue == nil then
    queue = {}
    queue.consumers = {}
    queue.workers = {}

    setmetatable(queue.consumers, {
            __index = function(tbs, space)
                local spt = {}
                setmetatable(spt, {
                    __index = function(tbt, tube)
                        local channel = box.ipc.channel(1)
                        rawset(tbt, tube, channel)

                        if queue.workers[space] == nil then
                            queue.workers[space] = {}
                        end

                        return channel
                    end
                })
                rawset(tbs, space, spt)
                return spt
            end
        }
    )

    setmetatable(queue.workers, {
            __index = function(tbs, space)
                local spt = {}
                setmetatable(spt, {
                    __index = function(tbt, tube)
                        local channel = box.ipc.channel(1)

                        local v = {
                            ch = channel,
                            fibers = {}
                        }


                        local fiber = box.fiber.create(
                            function()
                                box.fiber.detach()
                                process_tube(space, tube)
                            end
                        )
                        box.fiber.resume(fiber)
                        table.insert(v.fibers, fiber)
                        
                        rawset(tbt, tube, v)
                        return v
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
    queue.stat.ttl = { total = 0 }
    queue.stat.ttl[ human_status[ST_DONE] ] = 0
    queue.stat.ttl[ human_status[ST_READY] ] = 0
    queue.stat.ttl[ human_status[ST_TAKEN] ] = 0


    queue.stat.ttr = 0
    queue.stat.race_cond = 0
    queue.stat.put = 0
    queue.stat.take = 0
    queue.stat.take_timeout = 0






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


queue.put = function(space, tube, ...)

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
            box.pack('l', 0),
            box.pack('l', 0)
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
            box.pack('l', 0),
            box.pack('l', 0)
        }
    end

    for i = 1, #utask do
        table.insert(task, utask[i])
    end

    task = box.insert(space, unpack(task))

    if delay == 0 and not queue.consumers[space][tube]:is_full() then
        queue.consumers[space][tube]:put(true)
    end
    if not queue.workers[space][tube].ch:is_full() then
        queue.workers[space][tube].ch:put(true)
    end

    return rettask(task)
end


queue.take = function(space, tube, timeout)

    if timeout == nil then
        timeout = 0
    else
        timeout = tonumber(timeout)
        if timeout < 0 then
            timeout = 0
        end
    end

    local started = box.time()

    while true do

        local task = box.select_limit(space, idx_tube, 0, 1, tube, ST_READY)
        if task ~= nil then

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
                    ST_TAKEN,

                    i_event,
                    box.pack('l', event),

                    i_cid,
                    box.session.id()
            )

            if not queue.workers[space][tube].ch:is_full() then
                queue.workers[space][tube].ch:put(true)
            end
            return rettask(task)
        end

        if timeout > 0 then
            now = box.time()
            if now < started + timeout then
                queue.consumers[space][tube]:get(started + timeout - now)
            else
                return
            end
        end
    end
end

queue.delete = function(space, id)
    return rettask(box.delete(space, id))
end

queue.ack = function(space, id)
    local task = box.select(space, idx_task, id)
    if task == nil then
        error('Task not found')
    end

    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end

    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it ack')
    end

    return queue.delete(space, id)
end

queue.done = function(space, task, ...)
    local tuple = box.select(space, 0, task)
    if tuple == nil then
        return
    end
    tuple = tuple:transform(i_task, #tuple, ...)
    tuple = tuple:transform(i_status, 1, ST_DONE)
    tuple = box.replace(space, tuple:unpack())
    if not queue.workers[space][tube].ch:is_full() then
        queue.workers[space][tube].ch:put(true)
    end
    return rettask(tuple)
end

queue.release = function(space, id, delay, pri, ttr, ttl)
    local task = box.select(space, idx_task, id)
    if task == nil then
        error('Task not found')
    end
    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end
    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it ack')
    end

    local tube = task[i_tube]

    local now = box.time64()

    if ttl == nil then
        ttl = box.unpack('l', task[i_ttl])
    else
        ttl = to_time64(tonumber(ttl))
    end

    if ttr == nil then
        ttr = box.unpack('l', task[i_ttr])
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
        pri = box.unpack('i', task[i_pri])
    else
        pri = tonumber(pri)
    end

    if delay > 0 then
        task = box.update(space,
            id,
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
        task = box.update(space,
            id,
            '=p=p=p=p=p=p',
            i_status,
            ST_READY,
            i_event,
            box.unpack('l', task[i_started]) + ttl,
            i_ttl,
            ttl,
            i_ttr,
            ttr,
            i_pri,
            pri,
            i_cid,
            0
        )
        if not queue.consumers[space][tube]:is_full() then
            queue.consumers[space][tube]:put(true)
        end
    end
    if not queue.workers[space][tube].ch:is_full() then
        queue.workers[space][tube].ch:put(true)
    end

    return rettask(task)
end

queue.meta = function(space, id)
    local task = box.select(space, 0, id)
    if task == nil then
        error('Task not found');
    end



    task = task:transform(i_task, #task - i_task)
        :transform(i_status, 1, human_status[ task[i_status] ])
    return task

end




queue.peek = function(space, id)
    local tuple = box.select(space, 0, id)
    return rettask(tuple)
end


