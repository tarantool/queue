local function parse_version(version)
    if version == nil then
        return nil
    end

    local major = nil
    local minor = nil
    local patch = ''
    local sep = ''

    for token in string.gmatch(tostring(version), "[^.]+") do
        if not major then
            major = token
        elseif not minor then
            minor = token
        else
            patch = patch .. sep .. token
            sep = '.'
        end
    end

    return {
        major = tonumber(major),
        minor = tonumber(minor),
        patch = patch
    }
end

local exports = {
    parse = parse_version
}

return exports