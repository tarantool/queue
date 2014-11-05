package = 'queue'
version = 'scm-1'
source  = {
    url    = 'git@github.com:tarantool/queue-collection.git',
    branch = 'master',
}
description = {
    summary  = "Queue collection for tarantool",
    homepage = 'https://github.com/tarantool/queue-collection',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',

    modules = {
        ['queue.abstract']                 = queue/abstract.lua,
        ['queue.abstract.state']           = queue/abstract/state.lua,
        ['queue.abstract.driver.fifottl']  = queue/abstract/driver/fifottl.lua,
        ['queue.abstract.driver.utubettl'] = queue/abstract/driver/utubettl.lua,
        ['queue.abstract.driver.fifo']     = queue/abstract/driver/fifo.lua,
        ['queue.abstract.driver.utube']    = queue/abstract/driver/utube.lua,
        ['queue']                          = queue.lua
    }
}

-- vim: syntax=lua
