local state    = require('queue.abstract.state')

local num_type = require('queue.compat').num_type
local str_type = require('queue.compat').str_type

local tube = {}
local method = {}

-- create space
function tube.create_space(space_name, opts)
    local space_opts         = {}
    local if_not_exists      = opts.if_not_exists or false
    space_opts.temporary     = opts.temporary or false
    space_opts.if_not_exists = if_not_exists
    space_opts.format = {
        [1] = {name = 'task_id', type = num_type()},
        [2] = {name = 'status', type = str_type()},
        [3] = {name = 'data', type = '*'}
    }

    local space = box.schema.create_space(space_name, space_opts)
    space:create_index('task_id', {
        type = 'tree',
        parts = {1, num_type()},
        if_not_exists = if_not_exists
    })
    space:create_index('status',  {
        type = 'tree',
        parts = {2, str_type(), 1, num_type()},
        if_not_exists = if_not_exists
    })
    return space
end

-- start tube on space
function tube.new(space, on_task_change)
    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space          = space,
        on_task_change = on_task_change,
    }, { __index = method })
    return self
end

-- normalize task: cleanup all internal fields
function method.normalize_task(self, task)
    return task
end

-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = max and max[1] + 1 or 0
    local task = self.space:insert{id, state.READY, data}
    self.on_task_change(task, 'put')
    return task
end

-- take task
function method.take(self)
    local task = self.space.index.status:min{state.READY}
    if task ~= nil and task[2] == state.READY then
        task = self.space:update(task[1], { { '=', 2, state.TAKEN } })
        self.on_task_change(task, 'take')
        return task
    end
end

-- touch task
function method.touch(self, id, ttr)
    error('fifo queue does not support touch')
end

-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task ~= nil then
        task = task:transform(2, 1, state.DONE)
    end
    self.on_task_change(task, 'delete')
    return task
end

-- release task
function method.release(self, id, opts)
    local task = self.space:update(id, {{ '=', 2, state.READY }})
    if task ~= nil then
        self.on_task_change(task, 'release')
    end
    return task
end

-- bury task
function method.bury(self, id)
    local task = self.space:update(id, {{ '=', 2, state.BURIED }})
    self.on_task_change(task, 'bury')
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
        self.on_task_change(task, 'kick')
    end
    return count
end

-- peek task
function method.peek(self, id)
    return self.space:get{id}
end

function method.truncate(self)
    self.space:truncate()
end

return tube
