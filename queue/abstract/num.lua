local function get_num_or_unsigned(version_string)
    local version = require('queue/abstract/version').parse(version_string)

    if version.major == 1 and version.minor < 7 then
        return 'num'
    else
        return 'unsigned'
    end
end

return {
    get_num_or_unsigned = get_num_or_unsigned
}