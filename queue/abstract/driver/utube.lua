local state    = require('queue.abstract.state')
local num_type = require('queue.compat').num_type
local str_type = require('queue.compat').str_type

local tube = {}
local method = {}

local i_status = 2

-- create space
function tube.create_space(space_name, opts)
    local space_opts         = {}
    local if_not_exists      = opts.if_not_exists or false
    space_opts.temporary     = opts.temporary or false
    space_opts.if_not_exists = if_not_exists
    space_opts.engine        = opts.engine or 'memtx'
    space_opts.format = {
        {name = 'task_id', type = num_type()},
        {name = 'status', type = str_type()},
        {name = 'utube', type = str_type()},
        {name = 'data', type = '*'}
    }

    -- id, status, utube, data
    local space = box.schema.create_space(space_name, space_opts)
    space:create_index('task_id', {
        type = 'tree',
        parts = {1, num_type()},
        if_not_exists = if_not_exists
    })
    space:create_index('status', {
        type = 'tree',
        parts = {2, str_type(), 1, num_type()},
        if_not_exists = if_not_exists
    })
    space:create_index('utube', {
        type = 'tree',
        parts = {2, str_type(), 3, str_type(), 1, num_type()},
        if_not_exists = if_not_exists
    })
    return space
end

-- start tube on space
function tube.new(space, on_task_change, opts)
    on_task_change = on_task_change or (function() end)
    local self = setmetatable({
        space          = space,
        concurrent     = opts.concurrent or 1,
        on_task_change = on_task_change,
    }, { __index = method })
    return self
end

-- normalize task: cleanup all internal fields
function method.normalize_task(self, task)
    return task and task:transform(3, 1)
end

-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = max and max[1] + 1 or 0
    local task = self.space:insert{id, state.READY, tostring(opts.utube), data}
    self.on_task_change(task, 'put')
    return task
end

-- check concurrency of a sub-queue
function method.is_throttled(self, utube)
  if self.concurrent == 1 then
      local taken = self.space.index.utube:min{state.TAKEN, utube}
      return taken ~= nil and taken[3] == utube
  elseif self.concurrent ~= 1/0 then
      local num_taken = self.space.index.utube:count{state.TAKEN, utube}
      return num_taken == self.concurrent
  end
  return false
end

-- take task
function method.take(self, opts)
    local task
    if opts and opts.utube then
        if not self:is_throttled(opts.utube) then
            local t = self.space.index.utube:min{state.READY, opts.utube}
            if t and t[2] == state.READY and t[3] == opts.utube then
                task = t
            end
        end
    else
        local utubes = {}
        for s, t in self.space.index.status:pairs(state.READY) do
            local utube = t[3]
            if not utubes[utube] then
                if not self:is_throttled(utube) then
                    task = t
                    break
                end
                utubes[utube] = true
            end
        end
    end
    if task then
        task = self.space:update(task[1], { { '=', 2, state.TAKEN } })
        self.on_task_change(task, 'take')
        return task
    end
end

-- touch task
function method.touch(self, id, ttr)
    error('utube queue does not support touch')
end

-- delete task
function method.delete(self, id)
    local task = self.space:get(id)
    self.space:delete(id)
    if task ~= nil then
        task = task:transform(2, 1, state.DONE)

        local neighbour = self.space.index.utube:min{state.READY, task[3]}
        self.on_task_change(task, 'delete')
        if neighbour then
            self.on_task_change(neighbour)
        end
    else
        self.on_task_change(task, 'delete')
    end
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
    if task ~= nil then
        local neighbour = self.space.index.utube:min{state.READY, task[3]}
        self.on_task_change(task, 'bury')
        if neighbour and neighbour[i_status] == state.READY then
            self.on_task_change(neighbour)
        end
    else
        self.on_task_change(task, 'bury')
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
