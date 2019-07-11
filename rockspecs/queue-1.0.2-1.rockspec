package = 'queue'
version = '1.0.2-1'
source  = {
    url = 'git://github.com/tarantool/queue.git',
    tag = '1.0.2',
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
        ['queue.abstract.driver.fifottl']    = 'queue/abstract/driver/fifottl.lua',
        ['queue.abstract.driver.utubettl']   = 'queue/abstract/driver/utubettl.lua',
        ['queue.abstract.driver.fifo']       = 'queue/abstract/driver/fifo.lua',
        ['queue.abstract.driver.utube']      = 'queue/abstract/driver/utube.lua',
        ['queue.abstract.driver.limfifottl'] = 'queue/abstract/driver/limfifottl.lua',
        ['queue.compat']                     = 'queue/compat.lua',
        ['queue']                            = 'queue/init.lua'
    }
}

-- vim: syntax=lua
