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
    queue.default.ttr = 86400 * 365 -- 1 year

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


    if tube == nil then
        error 'You should define tube name'
    end

    
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


queue.service.process_tube = function(space, tube, channel)
    if queue.tube[tube] == nil then
        return false
    end
    if queue.tube[tube][space] == nil then
        return false
    end
    if not queue.tube[tube][space]:has_readers() then
        return false
    end

    

end



queue.service.worker = function()
    box.fiber.detach()
    box.fiber.yield()
    while(true) do
        for i, space in pairs(queue.tube) do
            for tube, channel in pairs(space) do
                queue.service.process_tube(space, tube, channel)
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


-- -- find ready tasks
-- local queue.get_ready_task = function(space, tube)
--     while true do
--         local task = box.select_range(space, 1, 1, tube, 'r')
--         if task == nil then
--             return
--         end
-- 
--         if box.unpack('i', task[i_event]) < os.time() then
--             local tr = box.unpack('i', task[i_ttr]) + os.time()
--             local tl = box.unpack('i', task[i_ttl]) + os.time()
--             local ne
--             if tr > tl then
--                 ne = tl
--             else
--                 ne = tr
--             end
--             return box.update(space, task[i_uuid], '=p', box.pack('i', ne))
--         end
-- 
--         -- task is expired
--         box.delete(space, task[0])
--     end
-- end
-- 
-- queue.service_fiber = function()
--     box.fiber.detach()
--     while(true) do
-- 
--         for space, tubes in pairs(queue.tube) do
--             for tube, ch in pairs(tubes) do
--                 while ch:has_readers() do
-- 
--                     local task = queue.get_ready_task(space, tube)
--                     if task == nil then
--                         break
--                     end
--                     ch:put(task)
-- 
--                 end
--             end
--         end
--         queue.service:get(.5)
--     end
-- end
-- 
-- 
-- queue.service_wakeup = function(tube)
--     -- run service fiber
--     if queue.fid == nil then
--         queue.fid = box.fiber.create(queue.service_fiber)
--         box.fiber.resume(queue.fid)
--     end
-- 
--     -- wakeup service fiber
--     if queue.service:has_readers() then
--         queue.service:put(tube)
--     end
-- end
-- 
-- queue.put = function(space, tube, delay, pri, ttl, ttr, body, ...)
--     local now = os.time()
--     ttl = tonumber(ttl)
--     if ttl <= 0 then
--         ttl = queue.default.ttl
--     end
--     ttr = tonumber(ttr)
--     if ttr <= 0 then
--         ttr = queue.default.ttr
--     end
--     delay = tonumber(delay)
-- 
--     local etime
--     local status
--     if delay > 0 then
--         ttl = ttl + delay
--         etime = now + delay
--         status = 'd'
--     else
--         etime = now + ttl
--         status = 'r'
--     end
-- 
--     local task
--     task[ i_uuid + 1    ] = box.uuid_hex()
--     task[ i_tube + 1    ] = tube
--     task[ i_status + 1  ] = status
--     task[ i_event + 1   ] = box.pack('i', etime)
--     task[ i_pri + 1     ] = box.pack('i', queue.default.pri + tonumber(pri))
--     task[ i_cid + 1     ] = ''
--     task[ i_started + 1 ] = box.pack('i', now)
--     task[ i_ttl + 1     ] = box.pack('i', ttl)
--     task[ i_ttr + 1     ] = box.pack('i', ttr)
--     task[ i_task + 1    ] = body
--     local args = { ... }
--     for i, a in pairs(args) do
--         table.insert(task, a)
--     end
-- 
--     task = box.insert(space, unpack(task))
-- 
--     if queue.tube[space] == nil then
--         queue.tube[space] = {}
--         queue.tube[space].tube = {}
--         queue.tube[space].tube[tube] = box.fiber.channel()
--     else
--         if queue.tube[space].tube[tube] == nil then
--             queue.tube[space].tube[tube] = box.fiber.channel()
--         end
--     end
-- 
--     queue.service_wakeup(tube)
--     return task[0]
-- end
