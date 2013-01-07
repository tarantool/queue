ch = box.ipc.channel(10)

function put(delay, data)
    if tonumber(delay) > 0 then
        box.fiber.sleep(tonumber(delay))
    end
    return ch:put(data)
end

function get(delay)
    if tonumber(delay) > 0 then
       box.fiber.sleep(tonumber(delay))
    end
    return ch:get()
end

function ping(...)
    return ...
end
