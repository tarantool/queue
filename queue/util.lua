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

--------------------------------------------------------
--------------------------------------------------------
-- Temporary helpers
local math = require('math')

local circular_buffer_methods = {}

local function circular_buffer_new(size)
    if size <= 0 then
        return nil
    end

    local self = setmetatable({
        size = size,
        index = 0,
        full = false,
        buffer = {}
    }, { __index = circular_buffer_methods })

    return self
end

function circular_buffer_methods.insert(self, element)
    self.index = math.fmod(self.index, self.size) + 1
    self.buffer[self.index] = element

    if not self.full and self.index == self.size then
        self.full = true
    end
end

function circular_buffer_methods.pick_next_write(self, element)
    if not self.full then
        return nil
    end

    index = math.fmod(self.index, self.size) + 1
    return self.buffer[index]
end

--------------------------------------------------------

local moving_avarage_calc_methods = {}

function moving_avarage_calc_new(size)
    if size <= 0 then
        return nil
    end

    local self = setmetatable({
        sum = 0,
        buffer = circular_buffer_new(size),
    }, { __index = moving_avarage_calc_methods })

    return self
end

function moving_avarage_calc_methods.insert(self, element)
    if self.buffer.full then
        self.sum = (self.sum + (element - self.buffer:pick_next_write()))
    else
        self.sum = (self.sum + element)
    end

    self.buffer:insert(element)
end

function moving_avarage_calc_methods.get_avarage(self)
    local  avg = nil

    if self.buffer.full then
        avg = self.sum / self.buffer.size
    elseif self.buffer.index > 0 then
        avg = self.sum / self.buffer.index
    end

    return avg
end
--------------------------------------------------------
--------------------------------------------------------

-- methods
local method = {
    time = time,
    event_time = event_time,
    --------------------------------------------------------
    --------------------------------------------------------
    moving_avarage_calc_new = moving_avarage_calc_new
    --------------------------------------------------------
    --------------------------------------------------------
}

return setmetatable(util, { __index = method })
