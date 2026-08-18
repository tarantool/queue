local fiber = require('fiber')

-- MAX_TIMEOUT == 100 years
local MAX_TIMEOUT      = 365 * 86400 * 100
-- Set to TIMEOUT_INFINITY
-- instead returns time for next event
local TIMEOUT_INFINITY = 18446744073709551615ULL

--- Convert seconds to microseconds.
-- If tm == nil then returns current system time
-- (in microseconds since the epoch).
-- If tm < 0 return 0.
local function time(tm)
    if tm == nil then
        tm = fiber.time64()
    elseif tm < 0 then
        tm = 0
    else
        tm = tm * 1000000
    end
    return 0ULL + tm
end

--- Calculates the system time (in microseconds) of an event that
-- will occur after a given time period(tm, specified in seconds).
-- If tm <= 0 then returns current system time.
local function event_time(tm)
    if tm == nil or tm < 0 then
        tm = 0
    elseif tm > MAX_TIMEOUT then
        return TIMEOUT_INFINITY
    end
    tm = 0ULL + tm * 1000000 + fiber.time64()
    return tm
end

local util = {
    MAX_TIMEOUT = MAX_TIMEOUT,
    TIMEOUT_INFINITY = TIMEOUT_INFINITY
}

local function atomic_tail(status, ...)
    if not status then
        box.rollback()
        error((...), 2)
     end

     box.commit()

     return ...
end

-- Analog of box.atomic() with checks for queue operations. 
-- It is used to wrap queue operations in a transaction.
local function atomic(fun, ...)
    if box.is_in_txn() then
        return fun(...)
    end

    if box.cfg.memtx_use_mvcc_engine then
        -- max() + insert() or min() + update() do not work as expected with
        -- best-effort visibility: for write transactions it chooses
        -- read-committed, for read transactions it chooses read-confirmed.
        --
        -- So max()/min() could return the same tuple even if a concurrent
        -- insert()/update() has been committed, but has not confirmed yet.
        box.begin({txn_isolation = 'read-committed'})
    else
        box.begin()
    end

    return atomic_tail(pcall(fun, ...))
end

-- methods
local method = {
    atomic = atomic,
    time = time,
    event_time = event_time
}

return setmetatable(util, { __index = method })
