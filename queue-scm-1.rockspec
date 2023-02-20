package = 'queue'
version = 'scm-1'
source  = {
    url    = 'git+https://github.com/tarantool/queue.git',
    branch = 'master',
}
description = {
    summary  = "A set of persistent in-memory queues",
    homepage = 'https://github.com/tarantool/queue.git',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',

    modules = {
        ['queue.abstract']                   = 'queue/abstract.lua',
        ['queue.abstract.state']             = 'queue/abstract/state.lua',
        ['queue.abstract.queue_session']     = 'queue/abstract/queue_session.lua',
        ['queue.abstract.queue_state']       = 'queue/abstract/queue_state.lua',
        ['queue.abstract.driver.fifottl']    = 'queue/abstract/driver/fifottl.lua',
        ['queue.abstract.driver.utubettl']   = 'queue/abstract/driver/utubettl.lua',
        ['queue.abstract.driver.fifo']       = 'queue/abstract/driver/fifo.lua',
        ['queue.abstract.driver.utube']      = 'queue/abstract/driver/utube.lua',
        ['queue.abstract.driver.limfifottl'] = 'queue/abstract/driver/limfifottl.lua',
        ['queue.compat']                     = 'queue/compat.lua',
        ['queue.util']                       = 'queue/util.lua',
        ['queue']                            = 'queue/init.lua',
        ['queue.version']                    = 'queue/version.lua'
    }
}

-- vim: syntax=lua
