local version = nil
local function parse_version()
    local major = nil
    local minor = nil

    for token in box.info.version:gmatch("\\.") do
        if not major then
            major = token
        elseif not minor then
            minor = token
        else
            break
        end
    end

    version = {
        major = tonumber(major),
        minor = tonumber(minor)
    }
end

local function get_type()
    if not version then
        parse_version()
    end

    if version.major == 1 and version.minor < 7 then
        return 'num'
    else
        return 'unsigned'
    end
end

return {
    get_type = get_type
}