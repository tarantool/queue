local fio   = require('fio')
local log   = require('log')
local yaml  = require('yaml')
local errno = require('errno')
local fiber  = require('fiber')
local netbox = require('net.box')

local dir     = os.getenv('QUEUE_TMP')
local cleanup = false

local qc              = require('queue.compat')
local vinyl_name      = qc.vinyl_name
local snapdir_optname = qc.snapdir_optname
local logger_optname  = qc.logger_optname

local bind_master  = os.getenv('QUEUE_MASTER_ADDR')
local bind_replica = os.getenv('QUEUE_REPLICA_ADDR')
local dir_replica  = nil
local replica      = nil

if bind_master == nil then
    bind_master = '127.0.0.1:3398'
end

if bind_replica == nil then
    bind_replica = '127.0.0.1:3399'
end

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

    cfg_args['wal_dir']         = dir
    cfg_args[snapdir_optname()] = dir
    cfg_args[logger_optname()]  = fio.pathjoin(dir, 'tarantool.log')
    if vinyl_name() then
        local vinyl_optname     = vinyl_name() .. '_dir'
        cfg_args[vinyl_optname] = dir
    end

    box.cfg(cfg_args)
end

-- Creates master and replica setup for queue states switching tests.
local function tnt_cluster_prepare(cfg_args)
    -- Since version 2.4.1, Tarantool has the popen built-in module
    -- that supports execution of external programs.
    if not qc.check_version({2, 4, 1}) then
        error('this test requires tarantool >= 2.4.1')
        return false
    end

    -- Prepare master.
    cfg_args = cfg_args or {}
    local files = fio.glob(fio.pathjoin(dir, '*'))
    for _, file in pairs(files) do
        if fio.basename(file) ~= 'tarantool.log' then
            log.info("skip removing %s", file)
            fio.unlink(file)
        end
    end

    cfg_args['wal_dir']         = dir
    cfg_args['read_only']       = false
    cfg_args[snapdir_optname()] = dir
    cfg_args[logger_optname()]  = fio.pathjoin(dir, 'tarantool.log')
    cfg_args['listen']          = bind_master
    cfg_args['replication']     = {'replicator:password@' .. bind_replica,
                                   'replicator:password@' .. bind_master}
    if vinyl_name() then
        local vinyl_optname     = vinyl_name() .. '_dir'
        cfg_args[vinyl_optname] = dir
    end
    cfg_args['replication_connect_quorum']  = 1
    cfg_args['replication_connect_timeout'] = 0.01

    box.cfg(cfg_args)
    -- Allow guest all operations.
    box.schema.user.grant('guest', 'read, write, execute', 'universe')
    box.schema.user.create('replicator', {password = 'password'})
    box.schema.user.grant('replicator', 'replication')

    -- Prepare replica.
    dir_replica = fio.tempdir()

    local vinyl_opt = nil
    if vinyl_name() then
        vinyl_opt = ', ' .. vinyl_name() .. '_dir = \'' .. dir_replica .. '\''
    else
        vinyl_opt = ''
    end

    local cmd_replica = {
        arg[-1],
        '-e',
        [[
        box.cfg {
            read_only = true,
            replication = 'replicator:password@]] .. bind_master ..
            '\', listen = \'' .. bind_replica ..
            '\', wal_dir = \'' .. dir_replica ..
            '\', ' .. snapdir_optname() .. ' = \'' .. dir_replica ..
            '\', ' .. logger_optname() .. ' = \'' ..
            fio.pathjoin(dir_replica, 'tarantool.log') .. '\'' ..
            vinyl_opt ..
        '}'
    }

    replica = require('popen').new(cmd_replica, {
        stdin = 'devnull',
        stdout = 'devnull',
        stderr = 'devnull',
    })

    -- Wait for replica to connect.
    local id = (box.info.replication[1].uuid ~= box.info.uuid and 1) or 2
    local attempts = 0

    while true do
        if #box.info.replication == 2 and box.info.replication[id].upstream then
            break
        end
        attempts = attempts + 1
        if attempts == 30 then
            error('wait for replica failed')
        end
        fiber.sleep(0.1)
    end
end

local function connect_replica()
    if not replica then
        return nil
    end

    return netbox.connect(bind_replica)
end

local function connect_master()
    return netbox.connect(bind_master)
end

-- Wait for replica to connect.
local function wait_replica()
    local attempts = 0

    while true do
        if #box.info.replication == 2 then
            return true
        end
        attempts = attempts + 1
        if attempts == 10 then
            return false
        end
        fiber.sleep(0.1)
    end

    return false
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
        if dir_replica then
            local files = fio.glob(fio.pathjoin(dir, '*'))
            for _, file in pairs(files) do
                log.info("remove %s", file)
                fio.unlink(file)
            end
        end
        if replica then
            replica:kill()
            replica:wait()
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

    cfg = tnt_prepare,
    cluster = {
        cfg = tnt_cluster_prepare,
        wait_replica = wait_replica,
        connect_replica = connect_replica,
        connect_master = connect_master
    }
}
