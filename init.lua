-- vim: set ft=lua et :

--  The library is free software; you can redistribute it and/or modify it
--  under the terms of either:

--  a) the GNU General Public License as published by the Free Software
--  Foundation; either version 1, or (at your option)
--  any later version (http://dev.perl.org/licenses/gpl1.html)

--  b) the "Artistic License" (http://dev.perl.org/licenses/artistic.html)

-- This library is free software; you can redistribute it and/or modify
-- it under the same terms as Perl itself (Artistic or GPL-1+).


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
--                         fieldno = 4,    # ipri
--                         type = "STR"
--                     },
--                     {
--                         fieldno = 5    # pri
--                         type = "STR"
--                     }
--                 ]
--             },
--             {
--                 type    = "TREE",
--                 unique  = 0,
--                 key_field = [
--                     {
--                         fieldno = 1,    # tube
--                         type = "STR"
--                     },
--                     {
--                         fieldno = 3,    # next_event
--                         type = "NUM64"
--                     }
--                 ]
--            },
--         ]
--     }
-- ]

-- If You want use method queue.put_unique you have to add additional
-- (fourth) index:
--            {
--                type    = "TREE",
--                unique  = 0,
--                key_field = [
--                    {
--                        fieldno = 1,    # tube
--                        type = "STR"
--                    },
--                     {
--                         fieldno = 2,    # status
--                         type = "STR"
--                     },
--                    {
--                        fieldno = 12,   # task data
--                        type = "STR"
--                    }
--                ]
--             }

-- Glossary
-- space - number of space contains tubes
-- tube - queue name
-- ttl - time to live
-- ttr - time to release (when task is run)
-- delay - delay period for task

(function(box)

local FIBERS_PER_TUBE  =   1

-- tuple structure
local i_uuid           = 0
local i_tube           = 1
local i_status         = 2
local i_event          = 3
local i_ipri           = 4
local i_pri            = 5
local i_cid            = 6
local i_created        = 7
local i_ttl            = 8
local i_ttr            = 9

local i_cbury          = 10
local i_ctaken         = 11
local i_task           = 12

local max_pri          = 0xFF
local min_pri          = 0
local med_pri          = 0x7F
local function pri_pack(pri)    return box.pack('b', pri)      end
local function pri_unpack(pri)  return box.unpack('b', pri)    end

-- indexes
local idx_task         = 0
local idx_tube         = 1
local idx_event        = 2
local idx_data         = 3

-- task statuses
local ST_READY         = 'r'
local ST_DELAYED       = 'd'
local ST_TAKEN         = 't'
local ST_BURIED        = 'b'
local ST_DONE          = '*'

-- timeout infinity
local TIMEOUT_INFINITY   = 365 * 86400


local human_status = {}
    human_status[ST_READY]      = 'ready'
    human_status[ST_DELAYED]    = 'delayed'
    human_status[ST_TAKEN]      = 'taken'
    human_status[ST_BURIED]     = 'buried'
    human_status[ST_DONE]       = 'done'

local all_statuses = {
    ST_READY,
    ST_DELAYED,
    ST_TAKEN,
    ST_BURIED,
    ST_DONE,
}


local function get_ipri(space, tube, inc)
    local toptask
    if inc < 0 then
        toptask = box.select_limit(space, idx_tube, 0, 1, tube, ST_READY)
        if toptask == nil then
            return queue.default.ipri
        end
    else
        toptask = box.select_reverse_range(space, idx_tube, 1, tube, ST_READY)
        if toptask == nil then
            return queue.default.ipri
        end
        if toptask[i_status] ~= ST_READY then
            return queue.default.ipri
        end
    end

    local ipri = pri_unpack(toptask[i_ipri])
    if inc < 0 then
        if ipri > min_pri - inc  then
            ipri = ipri + inc
        else
            ipri = min_pri
        end
    else
        if ipri < max_pri - inc then
            ipri = ipri + inc
        else
            ipri = max_pri
        end
    end

    return ipri
end

local function to_time64(time)
    return time * 1000000
end

local function from_time64(time)
    return tonumber(time) / 1000000
end

local function rettask(task)
    if task == nil then
        return
    end

    return task
        :transform(i_event, i_task - i_event)
        :transform(i_status, 1, human_status[ task[i_status] ])
end


local function process_tube(space, tube)

    while true do
        local now = box.time64()
        local next_event = 3600
        while true do
            local task = box.select_limit(space, idx_event, 0, 1, tube)

            if task == nil then
                break
            end

            local event = box.unpack('l', task[i_event])
            if event > now then
                next_event = from_time64(event - now)
                break
            end

            local created   = box.unpack('l', task[i_created])
            local ttl       = box.unpack('l', task[i_ttl])
            local ttr       = box.unpack('l', task[i_ttr])

            if now >= created + ttl then
                queue.stat[space][tube]:inc('ttl.total')
                queue.stat[space][tube]:inc(
                    'ttl.' .. human_status[task[i_status]])
                box.delete(space, task[i_uuid])

            -- delayed -> ready
            elseif task[i_status] == ST_DELAYED then
                box.update(space, task[i_uuid],
                    '=p=p',

                    i_status,
                    ST_READY,

                    i_event,
                    created + ttl
                )
                queue.consumers[space][tube]:put(true, 0)
            -- taken -> ready
            elseif task[i_status] == ST_TAKEN then
                box.update(space, task[i_uuid],
                    '=p=p=p',

                    i_status,
                    ST_READY,

                    i_cid,
                    0,

                    i_event,
                    created + ttl
                )
                queue.consumers[space][tube]:put(true, 0)
            else
                print("Internal error: unexpected task status: ",
                    task[i_status],
                    " (", human_status[ task[i_status] ], ")")
                box.update(space, task[i_uuid],
                    '=p',
                    i_event,
                    now + to_time64(5)
                )
            end
            now = box.time64()
        end

        queue.workers[space][tube].ch:get(next_event)
    end
end

local function consumer_dead_tube(space, tube, cid)
    local index = box.space[space].index[idx_tube]

    for task in index:iterator(box.index.EQ, tube, ST_TAKEN) do
        local created = box.unpack('l', task[i_created])
        local ttl = box.unpack('l', task[i_ttl])
        if box.unpack('i', task[i_cid]) == cid then
            queue.stat[space][tube]:inc('ready_by_disconnect')
            box.update(
                space,
                task[i_uuid],
                '=p=p=p',

                i_status,
                ST_READY,

                i_event,
                created + ttl,

                i_cid,
                0
            )


            queue.consumers[space][tube]:put(true, 0)
            queue.workers[space][tube].ch:put(true, 0)
        end
    end
end


local function consumer_dead(cid)
    for space, tbt in pairs(queue.consumers) do
        for tube, ch in pairs(tbt) do
            consumer_dead_tube(space, tube, cid)
        end
    end
end



if queue == nil then
    queue = {}
    queue.consumers = {}
    queue.workers = {}
    queue.stat = {}
    queue.restart = {}

    setmetatable(queue.consumers, {
            __index = function(tbs, space)
                local spt = {}
                setmetatable(spt, {
                    __index = function(tbt, tube)
                        local channel = box.ipc.channel(1)
                        rawset(tbt, tube, channel)
                        queue.restart_check(space, tube)
                        return channel
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
    
    setmetatable(queue.stat, {
            __index = function(tbs, space)
                local spt = {}
                setmetatable(spt, {
                    __index = function(tbt, tube)
                        local stat = {
                            inc = function(t, cnt)
                                t[cnt] = t[cnt] + 1
                                return t[cnt]
                            end
                        }
                        setmetatable(stat, {
                            __index = function(t, cnt)
                                rawset(t, cnt, 0)
                                return 0
                            end

                        })

                        rawset(tbt, tube, stat)
                        return stat
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

    setmetatable(queue.workers, {
            __index = function(tbs, space)
                
                local spt = rawget(tbs, space)
                spt = {}
                setmetatable(spt, {
                    __index = function(tbt, tube)
                        
                        local v = rawget(tbt, tube)

                        v = { fibers = {}, ch = box.ipc.channel(1) }

                        -- rawset have to be before start fiber
                        rawset(tbt, tube, v)
                        
                        queue.restart_check(space, tube)

                        for i = 1, FIBERS_PER_TUBE do
                            local fiber = box.fiber.create(
                                function()
                                    box.fiber.detach()
                                    process_tube(space, tube)
                                end
                            )
                            box.fiber.resume(fiber)
                            table.insert(v.fibers, fiber)
                        end
                        
                        return v
                    end
                })
                rawset(tbs, space, spt)
                return spt
            end,

            __gc = function(tbs)
                for space, tubes in pairs(tbs) do
                    for tube, tbt in pairs(tubes) do
                        for i, fiber in pairs(tbt.fibers) do
                            box.fiber.cancel(fiber)
                        end
                        tbt.fibers = nil
                        tbt.ch = nil
                        rawset(tubes, tube, nil)
                    end
                    rawset(tbs, space, nil)
                end
            end
        }
    )
end

queue.default = {}
    queue.default.pri   = med_pri
    queue.default.ipri  = med_pri
    queue.default.ttl   = 3600 * 24 * 25
    queue.default.ttr   = 60
    queue.default.delay = 0



queue.restart_check = function(space, tube)
    space = tonumber(space)
    if queue.restart[space] ~= nil and queue.restart[space][tube] then
        return 'already started'
    end

    if queue.restart[space] == nil then
        queue.restart[space] = {}
    end

    if queue.restart[space][tube] == nil then
        queue.restart[space][tube] = true
    end


    local fiber = box.fiber.create(
        function()
            box.fiber.detach()
            local wakeup = false
            local index = box.space[space].index[idx_tube]
            for task in index:iterator(box.index.EQ, tube, ST_TAKEN) do
                local now = box.time64()
                local event = box.unpack('l', task[i_event])
                local ttr = box.unpack('l', task[i_ttr])
                local ttl = box.unpack('l', task[i_ttl])
                local created = box.unpack('l', task[i_created])


                -- find task that is taken before tarantool
                if event - ttr <= now - to_time64(box.info.uptime)
                    or not box.session.exists(box.unpack('i', task[i_cid]))
                then
                    print(
                        'task id=', task[i_uuid],
                        ' (space=', space, ', tube=', tube, ') ',
                        'will be ready by restarting tarantool'
                    )
                    queue.stat[space][tube]:inc('ready_by_restart')
                    box.update(
                        space,
                        task[i_tube],
                        '=p=p=p',
                        i_status,
                        ST_READY,

                        i_event,
                        created + ttl,

                        i_cid,
                        0
                    )
                    wakeup = true
                end
            end
            if wakeup then
                queue.consumers[space][tube]:put(true, 0)
                queue.workers[space][tube].ch:put(true, 0)
            end
        end
    )
    box.fiber.resume(fiber)
    return 'starting'
end


local function put_statistics(stat, space, tube)
    if space == nil then
        return
    end
    if tube == nil then
        return
    end

    local st = rawget(queue.stat, space)
    if st == nil then
        return
    end

    st = rawget(st, tube)
    if st == nil then
        return
    end
    for name, value in pairs(st) do
        if type(value) ~= 'function' then

            table.insert(stat,
                'space' .. tostring(space) .. '.' .. tostring(tube)
                    .. '.' .. tostring(name)
            )
            table.insert(stat, tostring(value))
        end

    end
    table.insert(stat,
                'space' .. tostring(space) .. '.' .. tostring(tube)
                    .. '.tasks.total'
    )
    table.insert(stat,
        tostring(box.space[space].index[idx_tube]:count(tube))
    )

    for i, s in pairs(all_statuses) do
        table.insert(stat,
                    'space' .. tostring(space) .. '.' .. tostring(tube)
                        .. '.tasks.' .. human_status[s]
        )
        table.insert(stat,
            tostring(
                box.space[space].index[idx_tube]:count(tube, s)
            )
        )
    end
end


-- queue.statistics
-- returns statistics about all queues
queue.statistics = function( space, tube )

    local stat = {}

    if space ~= nil and tube ~= nil then
        space = tonumber(space)
        put_statistics(stat, space, tube)
    elseif space ~= nil then
        space = tonumber(space)
        local spt = rawget(queue.stat, space)
        if spt ~= nil then
            for tube, st in pairs(spt) do
                put_statistics(stat, space, tube)
            end
        end
    else
        for space, spt in pairs(queue.stat) do
            for tube, st in pairs(spt) do
                put_statistics(stat, space, tube)
            end
        end
    end
    return stat

end


local function put_task(space, tube, ipri, delay, ttl, ttr, pri, ...)

    ttl = tonumber(ttl)
    if ttl <= 0 then
        ttl = queue.default.ttl
    end
    ttl     = to_time64(ttl)

    delay = tonumber(delay)
    if delay <= 0 then
        delay = queue.default.delay
    end
    delay = to_time64(delay)
    ttl = ttl + delay


    ttr = tonumber(ttr)
    if ttr <= 0 then
        ttr = queue.default.ttr
    end
    ttr = to_time64(ttr)

    pri = tonumber(pri)
    pri = pri + queue.default.pri
    if pri > max_pri then
        pri = max_pri
    elseif pri < min_pri then
        pri = min_pri
    end

    pri = max_pri - (pri - min_pri)

    local task
    local now = box.time64()

    if delay > 0 then
        task = box.insert(space,
            box.uuid_hex(),
            tube,
            ST_DELAYED,
            box.pack('l', now + delay),
            pri_pack(ipri),
            pri_pack(pri),
            box.pack('i', 0),
            box.pack('l', now),
            box.pack('l', ttl),
            box.pack('l', ttr),
            box.pack('l', 0),
            box.pack('l', 0),
            ...
        )
    else
        task = box.insert(space,
            box.uuid_hex(),
            tube,
            ST_READY,
            box.pack('l', now + ttl),
            pri_pack(ipri),
            pri_pack(pri),
            box.pack('i', 0),
            box.pack('l', now),
            box.pack('l', ttl),
            box.pack('l', ttr),
            box.pack('l', 0),
            box.pack('l', 0),
            ...
        )
        queue.consumers[space][tube]:put(true, 0)
    end

    queue.workers[space][tube].ch:put(true, 0)

    return rettask(task)

end


-- queue.put(space, tube, delay, ttl, ttr, pri, ...)
--  put task into queue.
--   arguments
--      1. tube - queue name
--      2. delay - delay before task can be taken
--      3. ttl - time to live (if delay > 0 ttl := ttl + delay)
--      4. ttr - time to release (when task is taken)
--      5. pri - priority
--      6. ... - task data
queue.put = function(space, tube, ...)
    space = tonumber(space)
    queue.stat[space][tube]:inc('put')
    return put_task(space, tube, queue.default.ipri, ...)
end


-- queue.put_unique(space, tube, delay, ttl, ttr, pri, ...)
--  put unique task into queue.
--   arguments
--      1. tube - queue name
--      2. delay - delay before task can be taken
--      3. ttl - time to live (if delay > 0 ttl := ttl + delay)
--      4. ttr - time to release (when task is taken)
--      5. pri - priority
--      6. ... - task data
queue.put_unique = function(space, tube, delay, ttl, ttr, pri, data, ...)
    space = tonumber(space)
    
    delay = tonumber(delay)
    if delay <= 0 then
        delay = queue.default.delay
    end
    delay = to_time64(delay)

    local check_status = ST_READY
    if delay > 0 then
        check_status = ST_DELAYED
    end
    
    if data == nil then
	error('Can not put unique task without data')
    end

    if box.space[space].index[idx_data] == nil then
        error("Tarantool have to be configured to use queue.put_unique method")
    end

    local task = box.select( space, idx_data, tube, check_status, data )
    if task ~= nil then 
	return rettask( task )
    end
    queue.stat[space][tube]:inc('put')
    return put_task(space, tube, queue.default.ipri, delay, ttl, ttr, pri, data, ...)
end


-- queue.urgent(space, tube, delay, ttl, ttr, pri, ...)
--  like queue.put but put task at begin of queue
queue.urgent = function(space, tube, delayed, ...)
    space = tonumber(space)
    delayed = tonumber(delayed)
    queue.stat[space][tube]:inc('urgent')

    -- TODO: may decrease ipri before put_task
    if delayed > 0 then
        return put_task(space, tube, queue.default.ipri, delayed, ...)
    end

    local ipri = get_ipri(space, tube, -1)
    return put_task(space, tube, ipri, delayed, ...)
end


-- queue.take(space, tube, timeout)
-- take task for processing
queue.take = function(space, tube, timeout)
    
    space = tonumber(space)
    
    if timeout == nil then
        timeout = TIMEOUT_INFINITY
    else
        timeout = tonumber(timeout)
        if timeout < 0 then
            error("Timeout can't be less then 0")
        end
    end

    local created = box.time()

    while true do

        local iterator = box.space[space].index[idx_tube]
                                :iterator(box.index.EQ, tube, ST_READY)

        for task in iterator do
            local now = box.time64()
            local created = box.unpack('l', task[i_created])
            local ttr = box.unpack('l', task[i_ttr])
            local ttl = box.unpack('l', task[i_ttl])
            local event = now + ttr
            if event > created + ttl then
                event = created + ttl
                -- tube started too late
                if event <= now then
                    break
                end
            end


            task = box.update(space,
                task[i_uuid],
                    '=p=p=p+p',
                    i_status,
                    ST_TAKEN,

                    i_event,
                    event,

                    i_cid,
                    box.session.id(),

                    i_ctaken,
                    1
            )

            queue.workers[space][tube].ch:put(true, 0)
            queue.consumers[space][tube]:put(true, 0)
            queue.stat[space][tube]:inc('take')
            return rettask(task)
        end

        if timeout == 0 then
            queue.stat[space][tube]:inc('take_timeout')
            return
        end

        if timeout > 0 then
            local now = box.time()
            if now < created + timeout then
                queue.consumers[space][tube]:get(created + timeout - now)
            else
                queue.stat[space][tube]:inc('take_timeout')
                return
            end
        else
            queue.consumers[space][tube]:get()
        end
    end
end


-- queue.truncate(space, tube)
queue.truncate = function(space, tube)
    space = tonumber(space)

    local index = box.space[space].index[idx_tube]
    local task_ids = {}

    for task in index:iterator(box.index.EQ, tube) do
        table.insert(task_ids, task[i_uuid])
    end

    for _, task_id in pairs(task_ids) do
        box.space[space]:delete(task_id)
    end

    return #task_ids
end


-- queue.delete(space, id)
--  deletes task from queue
queue.delete = function(space, id)
    space = tonumber(space)
    local task = box.select(space, idx_task, id)
    if task == nil then
        error("Task not found")
    end

    queue.stat[space][ task[i_tube] ]:inc('delete')
    return rettask(box.delete(space, id))
end


-- queue.ack(space, id)
--  done task processing (task will be deleted)
queue.ack = function(space, id)
    space = tonumber(space)
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

    queue.stat[space][ task[i_tube] ]:inc('ack')
    return rettask(box.delete(space, id))
end


-- queue.touch(space, id)
--  prolong ttr for taken task
queue.touch = function(space, id)
    space = tonumber(space)
    local task = box.select(space, idx_task, id)
    if task == nil then
        error('Task not found')
    end

    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end

    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it touch')
    end

    local ttr = box.unpack('l', task[i_ttr])
    local ttl = box.unpack('l', task[i_ttl])
    local created = box.unpack('l', task[i_created])
    local now = box.time64()
    
    local event

    if created + ttl > now + ttr then
        event = now + ttr
    else
        event = created + ttl
    end

    task = box.update(space, id, '=p', i_event, event)

    queue.stat[space][ task[i_tube] ]:inc('touch')
    return rettask(task)
end


-- queue.done(space, id, ...)
--  marks task as done, replaces task's data
queue.done = function(space, id, ...)
    space = tonumber(space)
    local task = box.select(space, 0, id)
    if task == nil then
        error("Task not found")
    end
    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end

    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it done')
    end

    local event = box.unpack('l', task[i_created]) +
        box.unpack('l', task[i_ttl])
    local tube = task[i_tube]

    task = task
                :transform(i_task, #task, ...)
                :transform(i_status, 1, ST_DONE)
                :transform(i_event, 1, event)

    task = box.replace(space, task:unpack())
    queue.workers[space][tube].ch:put(true, 0)
    queue.stat[space][ tube ]:inc('done')
    return rettask(task)
end


-- queue.bury(space, id)
--  bury task that is taken
queue.bury = function(space, id)
    space = tonumber(space)
    local task = box.select(space, 0, id)
    if task == nil then
        error("Task not found")
    end
    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end

    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it done')
    end

    local event = box.unpack('l', task[i_created]) +
        box.unpack('l', task[i_ttl])
    local tube = task[i_tube]

    task = box.update(space, task[i_uuid],
        '=p=p+p',

        i_status,
        ST_BURIED,

        i_event,
        event,

        i_cbury,
        1
    )

    queue.workers[space][tube].ch:put(true, 0)
    queue.stat[space][ tube ]:inc('bury')
    return rettask(task)
end


-- queue.dig(space, id)
--  dig(unbury) task
queue.dig = function(space, id)
    
    space = tonumber(space)
    local task = box.select(space, 0, id)
    if task == nil then
        error("Task not found")
    end
    if task[i_status] ~= ST_BURIED then
        error('Task is not buried')
    end

    local tube = task[i_tube]

    task = box.update(space, task[i_uuid],
        '=p+p',

        i_status,
        ST_READY,
        
        i_cbury,
        1
    )

    queue.workers[space][tube].ch:put(true, 0)
    queue.stat[space][ tube ]:inc('dig')
    return rettask(task)
end

queue.unbury = queue.dig


-- queue.kick(space, tube, count)
queue.kick = function(space, tube, count)
    space = tonumber(space)
    
    local index = box.space[space].index[idx_tube]

    if count == nil then
        count = 1
    end
    count = tonumber(count)

    if count <= 0 then
        return 0
    end

    local kicked = 0
    
    for task in index:iterator(box.index.EQ, tube, ST_BURIED) do
        box.update(space, task[i_uuid],
            '=p+p',

            i_status,
            ST_READY,
            
            i_cbury,
            1
        )
        kicked = kicked + 1
        queue.stat[space][ tube ]:inc('dig')
    end

    return kicked
end


-- queue.release(space, id [, delay [, ttl ] ])
--  marks task as ready (or delayed)
queue.release = function(space, id, delay, ttl)
    space = tonumber(space)
    local task = box.select(space, idx_task, id)
    if task == nil then
        error('Task not found')
    end
    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end
    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it release')
    end

    local tube = task[i_tube]

    local now = box.time64()
    local created = box.unpack('l', task[i_created])

    if delay == nil then
        delay = 0
    else
        delay = to_time64(tonumber(delay))
        if delay <= 0 then
            delay = 0
        end
    end
    
    if ttl == nil then
        ttl = box.unpack('l', task[i_ttl])
    else
        ttl = tonumber(ttl)
        if ttl > 0 then
            ttl = to_time64(ttl)
            ttl = ttl + now - created
            if delay > 0 then
                ttl = ttl + delay
            end
        else
            ttl = box.unpack('l', task[i_ttl])
        end
    end
    



    if delay > 0 then
        local event = now + delay
        if event > created + ttl then
            event = created + ttl
        end

        task = box.update(space,
            id,
            '=p=p=p=p',

            i_status,
            ST_DELAYED,

            i_event,
            event,

            i_ttl,
            ttl,

            i_cid,
            0
        )
    else
        task = box.update(space,
            id,
            '=p=p=p=p',

            i_status,
            ST_READY,

            i_event,
            created + ttl,

            i_ttl,
            ttl,

            i_cid,
            0
        )
        queue.consumers[space][tube]:put(true, 0)
    end
    queue.workers[space][tube].ch:put(true, 0)
   

    queue.stat[space][ task[i_tube] ]:inc('release')

    return rettask(task)
end


-- queue.requeue(space, id)
--  marks task as ready and push it at end of queue
queue.requeue = function(space, id)
    space = tonumber(space)
    local task = box.select(space, idx_task, id)
    if task == nil then
        error('Task not found')
    end
    if task[i_status] ~= ST_TAKEN then
        error('Task is not taken')
    end
    if box.unpack('i', task[i_cid]) ~= box.session.id() then
        error('Only consumer that took the task can it release')
    end

    local tube = task[i_tube]

    local now = box.time64()


    local ipri = get_ipri(space, tube, 1)

    local created = box.unpack('l', task[i_created])
    local ttl = box.unpack('l', task[i_ttl])


    task = box.update(space,
        id,
        '=p=p=p=p',

        i_status,
        ST_READY,

        i_event,
        created + ttl,

        i_cid,
        0,
        
        i_ipri,
        pri_pack(ipri)
    )
    queue.consumers[space][tube]:put(true, 0)
    queue.workers[space][tube].ch:put(true, 0)
    
    queue.stat[space][ task[i_tube] ]:inc('requeue')

    return rettask(task)
end


-- queue.meta(space, id)
--  metainformation about task
--  returns:
--      1.  uuid:str
--      2.  tube:str
--      3.  status:str
--      4.  event:time64
--      5.  ipri:num(str)
--      6.  pri:num(str)
--      7.  cid:num
--      8.  created:time64
--      9.  ttl:time64
--      10. ttr:time64
--      11. cbury:num
--      12. ctaken:num
--      13. now:time64
queue.meta = function(space, id)
    space = tonumber(space)
    local task = box.select(space, 0, id)
    if task == nil then
        error('Task not found');
    end

    queue.stat[space][ task[i_tube] ]:inc('meta')

    task = task
        :transform(i_pri, 1, tostring(pri_unpack(task[i_pri])))
        :transform(i_ipri, 1, tostring(pri_unpack(task[i_ipri])))
        :transform(i_task, #task - i_task, box.time64())
        :transform(i_status, 1, human_status[ task[i_status] ])
    return task
end


-- queue.peek(space, id)
-- peek task
queue.peek = function(space, id)
    space = tonumber(space)
    local task = box.select(space, 0, id)
    if task == nil then
        error("Task not found")
    end

    queue.stat[space][ task[i_tube] ]:inc('peek')
    return rettask(task)
end

box.session.on_disconnect( function() consumer_dead(box.session.id()) end )


end)(box)
