-- Queue states.

local log   = require('log')
local fiber = require('fiber')

--[[ States switching scheme:

                       +-----------+
                       |  RUNNING  |
                       +-----------+
                        ^         | (rw -> ro)
      (ro -> rw)        |         v
+------+       +---------+       +--------+
| INIT | --->  | STARTUP |       | ENDING |
+------+       +---------+       +--------+
                        ^         |
             (ro -> rw) |         v
                       +-----------+
                       |  WAITING  |
                       +-----------+
]]--

local queue_state = {
    states = {
        INIT    = 'i',
        STARTUP = 's',
        RUNNING = 'r',
        ENDING  = 'e',
        WAITING = 'w',
    }
}

local current = queue_state.states.INIT

local function get_state_key(value)
    for k, v in pairs(queue_state.states) do
        if v == value then
            return k
        end
    end

    return nil
end

local function max_lag()
    local max_lag = 0

    for i = 1, #box.info.replication do
        if box.info.replication[i].upstream then
            local lag = box.info.replication[i].upstream.lag
            if lag > max_lag then
                max_lag = lag
            end
        end
    end

    return max_lag
end

local function create_state_fiber(on_state_change_cb)
    log.info('Started queue state fiber')

    fiber.create(function()
        fiber.self():name('queue_state_fiber')
        while true do
            if current == queue_state.states.WAITING then
                local rc = pcall(box.ctl.wait_rw, 0.001)
                if rc then
                    current = queue_state.states.STARTUP
                    log.info('Queue state changed: STARTUP')
                    -- Wait for maximum upstream lag * 2.
                    fiber.sleep(max_lag() * 2)
                    on_state_change_cb(current)
                    current = queue_state.states.RUNNING
                    log.info('Queue state changed: RUNNING')
                end
            elseif current == queue_state.states.RUNNING then
                local rc = pcall(box.ctl.wait_ro, 0.001)
                if rc then
                    current = queue_state.states.ENDING
                    on_state_change_cb(current)
                    log.info('Queue state changed: ENDING')
                    current = queue_state.states.WAITING
                    log.info('Queue state changed: WAITING')
                end
            end
        end
    end)
end

-- Public methods.
local method = {}

-- Initialise queue states. Must be called on queue starts.
function method.init(tubes)
    current = queue_state.states.RUNNING
    create_state_fiber(tubes)
end

-- Show current state in human readable format.
function method.show()
    return get_state_key(current)
end

-- Get current state.
function method.get()
    return current
end

-- Polls queue state with maximum attemps number.
function method.poll(state, max_attempts)
    local attempt = 0
    while true do
        if current == state then
            return true
        end
        attempt = attempt + 1
        if attempt == max_attempts then
            break
        end
        fiber.sleep(0.01)
    end

    return false
end

return setmetatable(queue_state, { __index = method })
