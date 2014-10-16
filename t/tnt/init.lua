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

local files = fio.glob(fio.pathjoin(dir, '*'))
for _, file in pairs(files) do
    if fio.basename(file) ~= 'tarantool.log' then
        log.info("skip removing %s", file)
        fio.unlink(file)
    end
end

box.cfg {
    wal_dir     = dir,
    snap_dir    = dir,
    sophia_dir  = dir,
    logger      = fio.pathjoin(dir, 'tarantool.log')
}

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
    end
}


