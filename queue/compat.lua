local fun  = require('fun')
local log  = require('log')
local json = require('json')

local iter, op  = fun.iter, fun.operator

function split(self, sep)
    local sep, fields = sep or ":", {}
    local pattern = string.format("([^%s]+)", sep)
    self:gsub(pattern, function(c) table.insert(fields, c) end)
    return fields
end

local function opge(l, r)
    l = type(l) == 'string' and tonumber(l) or l
    r = type(r) == 'string' and tonumber(r) or r
    return l >= r
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
    return iter(version):zip(expected):every(opge)
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

local function pack_args(...)
    return check_version({1, 7}) and { ... } or ...
end

return {
    split_version   = split_version,
    check_version   = check_version,
    vinyl_name      = get_actual_vinylname,
    num_type        = get_actual_numtype,
    str_type        = get_actual_strtype,
    snapdir_optname = get_optname_snapdir,
    logger_optname  = get_optname_logger,
    pack_args       = pack_args,
}
