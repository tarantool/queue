local state = require 'queue.abstract.state'
local tube = {}
local method = {}


-- create space
function tube.create_space(space_name)
    local space = box.schema.create_space(space_name)
    space:create_index('task_id', { type = 'tree', parts = { 1, 'num' }})
    space:create_index('status', { type = 'tree', parts = { 2, 'str', 1, 'num' }})
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


-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = 0
    if max ~= nil then
        id = max[1] + 1
    end
    local task = self.space:insert{id, state.READY, data}
    self.on_task_change(task)
    return task
end


-- take task
function method.take(self)
    local task = self.space.index.status:min{state.READY}
    if task ~= nil and task[2] == state.READY then
        return self.space:update(task[1], { { '=', 2, state.TAKEN } })
    end
end

-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task then
        self.on_task_change()
        return task
    else
        box.error(box.error.PROC_LUA, "Task not found")
    end
end

-- release task
function method.release(self, id, opts)
    local task = self.space:update(id, {{ '=', 2, state.READY }})
    if task == nil then
        box.error(box.error.PROC_LUA, "Task not found")
    end
    self.on_task_change(task)
    return task
end

return tube
