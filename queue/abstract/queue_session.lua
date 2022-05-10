local log      = require('log')
local fiber    = require('fiber')
local uuid     = require('uuid')

local util     = require('queue.util')
local qc       = require('queue.compat')
local num_type = qc.num_type
local str_type = qc.str_type


local queue_session = {}

--
-- Replicaset mode switch.
--
-- Running a queue in master-replica mode requires that
-- `_queue_shared_sessions` space was not temporary.
-- When the queue is running in single mode,
-- the space is converted to temporary mode to increase performance.
--
local function switch_in_replicaset(replicaset_mode)
    if replicaset_mode == nil then
        replicaset_mode = false
    end

    if not box.space._queue_shared_sessions then
        return
    end

    if box.space._queue_shared_sessions.temporary
        and replicaset_mode == false then
        return
    end

    if not box.space._queue_shared_sessions.temporary
        and replicaset_mode == true then
        return
    end

    box.schema.create_space('_queue_shared_sessions_mgr', {
        temporary = not replicaset_mode,
        format = {
            { name = 'uuid', type = str_type() },
            { name = 'exp_time', type = num_type() },
            { name = 'active', type = 'boolean' },
        }
    })

    box.space._queue_shared_sessions_mgr:create_index('uuid', {
        type = 'tree',
        parts = { 1, str_type() },
        unique = true
    })
    box.space._queue_shared_sessions_mgr:create_index('active', {
        type = 'tree',
        parts = { 3, 'boolean', 1, str_type() },
        unique = true
    })

    box.begin() -- Disable implicit yields until the transaction ends.
    for _, tuple in box.space._queue_shared_sessions:pairs() do
        box.space._queue_shared_sessions_mgr:insert(tuple)
    end

    box.space._queue_shared_sessions:drop()
    box.space._queue_shared_sessions_mgr:rename('_queue_shared_sessions')

    local status, err = pcall(box.commit)
    if not status then
        error(('Error migrate _queue_shared_sessions: %s'):format(tostring(err)))
    end
end

--- Create everything that's needed to work with "shared" sessions.
local function identification_init()
    local queue_session_ids = box.space._queue_session_ids
    if queue_session_ids == nil then
        queue_session_ids = box.schema.create_space('_queue_session_ids', {
            temporary = true,
            format = {
                { name = 'connection_id', type = num_type() },
                { name = 'session_uuid', type = str_type() }
            }
        })

        queue_session_ids:create_index('conn_id', {
            type = 'tree',
            parts = {1, num_type()},
            unique = true
        })
        queue_session_ids:create_index('uuid', {
            type = 'tree',
            parts = {2, str_type()},
            unique = false
        })
    end

    local replicaset_mode = queue_session.cfg['in_replicaset'] or false
    if box.space._queue_shared_sessions == nil then
        box.schema.create_space('_queue_shared_sessions', {
            temporary = not replicaset_mode,
            format = {
                { name = 'uuid', type = str_type() },
                { name = 'exp_time', type = num_type() },
                { name = 'active', type = 'boolean' },
            }
        })

        box.space._queue_shared_sessions:create_index('uuid', {
            type = 'tree',
            parts = { 1, str_type() },
            unique = true
        })
        box.space._queue_shared_sessions:create_index('active', {
            type = 'tree',
            parts = { 3, 'boolean', 1, str_type() },
            unique = true
        })
    else
        switch_in_replicaset(queue_session.cfg['in_replicaset'])

        -- At the start of the queue, we make all active sessions inactive,
        -- and set an expiration time for them. If the ttr setting is not set,
        -- then we delete all sessions.
        local ttr = queue_session.cfg['ttr'] or 0
        if ttr > 0 then
            for _, tuple in box.space._queue_shared_sessions.index.active:pairs{true} do
                    box.space._queue_shared_sessions:update(tuple[1], {
                        {'=', 2, util.event_time(ttr)},
                        {'=', 3, false},
                    })
            end
        else
            if queue_session._on_session_remove ~= nil then
                for _, tuple in box.space._queue_shared_sessions.index.uuid:pairs() do
                    queue_session._on_session_remove(tuple[1])
                end
            end
            box.space._queue_shared_sessions:truncate()
        end
    end
end

local function cleanup_inactive_sessions()
    local cur_time = util.time()

    for _, val in box.space._queue_shared_sessions.index.active:pairs{false} do
        local session_uuid = val[1]
        local exp_time = val[2]
        if cur_time >= exp_time then
            if queue_session._on_session_remove ~= nil then
                queue_session._on_session_remove(session_uuid)
            end
            box.space._queue_shared_sessions:delete{session_uuid}
        end
    end
end

--- Create an expiration fiber to cleanup expired sessions.
local function create_expiration_fiber()
    local exp_fiber = fiber.create(function()
        fiber.self():name('queue_expiration_fiber')
        while true do
            if box.info.ro == false then
                local status, err = pcall(cleanup_inactive_sessions)
                if status == false then
                    log.error('An error occurred while cleanup the sessions: %s',
                        err)
                end
            end
            if queue_session.sync_chan:get(1) ~= nil then
                log.info("Queue expiration fiber was stopped")
                break
            end
        end
    end)

    return exp_fiber
end

--- Identifies the connection and return the UUID of the current session.
-- If session_uuid ~= nil: associate the connection with given session.
-- In case of attempt to use an invalid format UUID or expired UUID,
-- an error will be thrown.
local function identify(conn_id, session_uuid)
    local queue_session_ids = box.space._queue_session_ids
    local session_ids = queue_session_ids:get(conn_id)
    local cur_uuid = session_ids and session_ids[2]

    if session_uuid == nil and cur_uuid ~= nil then
        -- Just return the UUID of the current session.
        return cur_uuid
    elseif session_uuid == nil and cur_uuid == nil then
        -- Generate new UUID for the session.
        cur_uuid = uuid.bin()
        queue_session_ids:insert{conn_id, cur_uuid}
        box.space._queue_shared_sessions:insert{cur_uuid, 0, true}
    elseif session_uuid ~= nil then
        -- Validate UUID.
        if not pcall(uuid.frombin, session_uuid) then
            error('Invalid UUID format.')
        end

        -- identify using a previously created session.
        -- Check that a session with this uuid exists.
        local ids_by_uuid = queue_session_ids.index.uuid:select(
            session_uuid, { limit = 1 })[1]
        local shared_session = box.space._queue_shared_sessions:get(session_uuid)
        if ids_by_uuid == nil and shared_session == nil then
            error('The UUID ' .. uuid.frombin(session_uuid):str() ..
                ' is unknown.')
        end

        if cur_uuid ~= session_uuid then
            if cur_uuid ~= nil then
                queue_session.disconnect(conn_id)
            end
            queue_session_ids:insert({conn_id, session_uuid})
            cur_uuid = session_uuid
        end

        -- Make session active.
        if shared_session then
            box.space._queue_shared_sessions:update(shared_session[1], {
                {'=', 2, 0},
                {'=', 3, true},
            })
        end
    end

    return cur_uuid
end

local function on_session_remove(callback)
    if type(callback) ~= 'function' then
        error('The "on_session_remove" argument type must be "function"')
    end
    queue_session._on_session_remove = callback
end

--- Remove a connection from the list of active connections and
-- release its tasks if necessary.
local function disconnect(conn_id)
    local queue_session_ids = box.space._queue_session_ids
    local session_uuid = queue_session.identify(conn_id)

    queue_session_ids:delete{conn_id}
    local session_ids = queue_session_ids.index.uuid:select(session_uuid,
        { limit = 1 })[1]

    -- If a queue session doesn't have any active connections it should be
    -- removed (if ttr is absent) or change the `active` flag to make the
    -- session inactive.
    if session_ids == nil then
        local ttr = queue_session.cfg['ttr'] or 0
        if ttr > 0 then
            local tuple = box.space._queue_shared_sessions:get{session_uuid}
            if tuple == nil then
                box.space._queue_shared_sessions:insert{
                    session_uuid, util.event_time(ttr), false
                }
            else
                box.space._queue_shared_sessions:update(tuple[1], {
                    {'=', 2, util.event_time(ttr)},
                    {'=', 3, false},
                })
            end
        elseif queue_session._on_session_remove ~= nil then
            queue_session._on_session_remove(session_uuid)
            box.space._queue_shared_sessions.index.uuid:delete{session_uuid}
        end
    end
end

local function grant(user)
    box.schema.user.grant(user, 'read, write', 'space', '_queue_session_ids',
        { if_not_exists = true })
    box.schema.user.grant(user, 'read, write', 'space', '_queue_shared_sessions',
        { if_not_exists = true })
end

local function start()
    identification_init()
    queue_session.sync_chan = fiber.channel()
    queue_session.expiration_fiber = create_expiration_fiber()
end

-- When switching the master to the replica,
-- the expiration_fiber must be stopped.
local function stop()
    queue_session.sync_chan:put(true)
end

local function validate_opts(opts)
    for key, val in pairs(opts) do
        if key == 'ttr' then
            if type(val) ~= 'number' or val < 0 then
                error('Invalid value of ttr: ' .. tostring(val))
            end
        elseif key == 'in_replicaset' then
            -- Do nothing.
        else
            error('Unknown option ' .. tostring(key))
        end
    end
end

--- Configure of queue_session module.
-- If an invalid value or an unknown option
-- is used, an error will be thrown.
local function cfg(self, opts)
    opts = opts or {}
    -- Check all options before configuring so that
    -- the configuration is done transactionally.
    validate_opts(opts)

    for key, val in pairs(opts) do
        self[key] = val
    end

    switch_in_replicaset(self['in_replicaset'])
end

local function exist_shared(session_uuid)
    if box.space._queue_shared_sessions:get{session_uuid} then
        return true
    end

    return false
end

queue_session.cfg = setmetatable({}, { __call = cfg })

-- methods
local method = {
    identify = identify,
    disconnect = disconnect,
    grant = grant,
    on_session_remove = on_session_remove,
    start = start,
    stop = stop,
    exist_shared = exist_shared
}

return setmetatable(queue_session, { __index = method })
