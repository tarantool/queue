local state = require 'queue.abstract.state'
local tube = {}
local method = {}
local log = require 'log'

-- create space
function tube.create_space(space_name, opts)
    local space_opts = {}
    if opts.temporary then
        space_opts.temporary = true
    end

    -- id, status, stube, data
    local space = box.schema.create_space(space_name, space_opts)
    space:create_index('task_id', { type = 'tree', parts = { 1, 'num' }})
    space:create_index('status',
        { type = 'tree', parts = { 2, 'str', 1, 'num' }})
    space:create_index('stube',
        { type = 'tree', parts = { 2, 'str', 3, 'str', 1, 'num' }})
    return space
end



-- start tube on space
function tube.new(space, on_task_change)
    if on_task_change == nil then
        on_task_change = function() end
    end
    local self = {
        space       = space,
        on_task_change = on_task_change,
    }
    setmetatable(self, { __index = method })
    return self
end


-- normalize task: cleanup all internal fields
function method.normalize_task(self, task)
    if task ~= nil then
        return task:transform(3, 1)
    end
end


-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = 0
    if max ~= nil then
        id = max[1] + 1
    end
    local task = self.space:insert{id, state.READY, tostring(opts.stube), data}
    self.on_task_change(task)
    return task
end


-- take task
function method.take(self, opts)
    local task = self.space.index.stube:min{state.READY, tostring(opts.stube)}
    if task ~= nil and task[2] == state.READY then
        task = self.space:update(task[1], { { '=', 2, state.TAKEN } })
        self.on_task_change(task)
        return task
    end
end

-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task ~= nil then
        task = task:transform(2, 1, state.DONE)

        local neighbour = self.space.index.stube:min{state.READY, task[3]}
        self.on_task_change(task)
        if neighbour then
            self.on_task_change(neighbour)
        end
    else
        self.on_task_change(task)
    end
    return task
end

-- release task
function method.release(self, id, opts)
    local task = self.space:update(id, {{ '=', 2, state.READY }})
    if task ~= nil then
        self.on_task_change(task)
    end
    return task
end

-- bury task
function method.bury(self, id)
    local task = self.space:update(id, {{ '=', 2, state.BURIED }})
    if task ~= nil then
        local neighbour = self.space.index.stube:min{state.READY, task[3]}
        self.on_task_change(task)
        if neighbour then
            self.on_task_change(neighbour)
        end
    else
        self.on_task_change(task)
    end
    return task
end

-- unbury several tasks
function method.kick(self, count)
    for i = 1, count do
        local task = self.space.index.status:min{ state.BURIED }
        if task == nil then
            return i - 1
        end
        if task[2] ~= state.BURIED then
            return i - 1
        end

        task = self.space:update(task[1], {{ '=', 2, state.READY }})
        self.on_task_change(task)
    end
    return count
end

-- peek task
function method.peek(self, id)
    return self.space:get{id}
end


return tube
