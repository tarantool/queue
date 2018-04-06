local fun   = require('fun')
local log   = require('log')
local json  = require('json')
local fiber = require('fiber')

local iter, op  = fun.iter, fun.operator

local function split(self, sep)
    local sep, fields = sep or ":", {}
    local pattern = string.format("([^%s]+)", sep)
    self:gsub(pattern, function(c) table.insert(fields, c) end)
    return fields
end

local function reducer(res, l, r)
    if res ~= nil then
        return res
    end
    if tonumber(l) == tonumber(r) then
        return nil
    end
    return tonumber(l) > tonumber(r)
end

local function split_version(version_string)
    local vtable  = split(version_string, '.')
    local vtable2 = split(vtable[3],  '-')
    vtable[3], vtable[4] = vtable2[1], vtable2[2]
    return vtable
end

local function check_version(expected, version)
    version = version or _TARANTOOL
    if type(version) == 'string' then
        version = split_version(version)
    end
    local res = iter(version):zip(expected):reduce(reducer, nil)
    if res or res == nil then res = true end
    return res
end

local function get_actual_numtype(version)
    return check_version({1, 7, 2}, version) and 'unsigned' or 'num'
end

local function get_actual_strtype(version)
    return check_version({1, 7, 2}, version) and 'string' or 'str'
end

local function get_actual_vinylname(version)
    return check_version({1, 7}, version) and 'vinyl' or nil
end

local function get_optname_snapdir(version)
    return check_version({1, 7}, version) and 'memtx_dir' or 'snap_dir'
end

local function get_optname_logger(version)
    return check_version({1, 7}, version) and 'log' or 'logger'
end

local function get_replication_id()
    -- box.info.server.id is compatible over all versions
    return box.info.main_replication_id or box.info.server.id
end

local function pack_args(...)
    return check_version({1, 7}) and { ... } or ...
end

local waiter_list = {}

local function waiter_new()
    return setmetatable({
        cond = fiber.cond()
    }, {
        __index = {
            wait = function(self, timeout)
                self.cond:wait(timeout)
            end,
            signal = function(self, wfiber)
                self.cond:signal()
            end,
            free = function(self)
                if #waiter_list < 100 then
                    table.insert(waiter_list, self)
                end
            end
        }
    })
end

local function waiter_old()
    return setmetatable({}, {
        __index = {
            wait = function(self, timeout)
                fiber.sleep(timeout)
            end,
            signal = function(self, fid)
                local wfiber = fiber.find(fid)
                if wfiber ~= nil and
                   wfiber:status() ~= 'dead' and
                   wfiber:id() ~= fiber.id() then
                    wfiber:wakeup()
                end
            end,
            free = function(self)
                if #waiter_list < 100 then
                    table.insert(waiter_list, self)
                end
            end
        }
    })
end

local waiter_actual = check_version({1, 7, 2}) and waiter_new or waiter_old

local function waiter()
    return table.remove(waiter_list) or waiter_actual()
end

return {
    split_version   = split_version,
    check_version   = check_version,
    vinyl_name      = get_actual_vinylname,
    num_type        = get_actual_numtype,
    str_type        = get_actual_strtype,
    snapdir_optname = get_optname_snapdir,
    logger_optname  = get_optname_logger,
    replication_id  = get_replication_id,
    pack_args       = pack_args,
    waiter          = waiter
}
