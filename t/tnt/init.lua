local fio = require 'fio'
local errno = require 'errno'
local yaml = require 'yaml'
local log = require 'log'

local dir = os.getenv('QUEUE_TMP')
local cleanup = false

if dir == nil then
    dir = fio.tempdir()
    cleanup = true
end

local function tnt_prepare(cfg_args)
    cfg_args = cfg_args or {}
    local files = fio.glob(fio.pathjoin(dir, '*'))
    for _, file in pairs(files) do
        if fio.basename(file) ~= 'tarantool.log' then
            log.info("skip removing %s", file)
            fio.unlink(file)
        end
    end

    cfg_args['wal_dir']    = dir
    cfg_args['snap_dir']   = dir
    cfg_args['logger']     = fio.pathjoin(dir, 'tarantool.log')

    box.cfg (cfg_args)
end

return {
    finish = function(code)
        local files = fio.glob(fio.pathjoin(dir, '*'))
        for _, file in pairs(files) do
            if fio.basename(file) == 'tarantool.log' and not cleanup then
                log.info("skip removing %s", file)
            else
                log.info("remove %s", file)
                fio.unlink(file)
            end
        end
        if cleanup then
            log.info("rmdir %s", dir)
            fio.rmdir(dir)
        end
    end,

    dir = function()
        return dir
    end,

    cleanup = function()
        return cleanup
    end,

    logfile = function()
        return fio.pathjoin(dir, 'tarantool.log')
    end,

    log = function()
        local fh = fio.open(fio.pathjoin(dir, 'tarantool.log'), 'O_RDONLY')
        if fh == nil then
            box.error(box.error.PROC_LUA, errno.strerror())
        end

        local data = fh:read(16384)
        fh:close()
        return data
    end,

    cfg = tnt_prepare
}


