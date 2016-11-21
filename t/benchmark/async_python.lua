#!/usr/bin/env tarantool

box.cfg{listen = 3301, wal_mode='none'}
box.schema.user.grant('guest', 'read,write,execute', 'universe')

queue = require('queue')
local fiber = require('fiber')

queue.create_tube('mail_msg', 'fifottl')

local producer = fiber.create(function()
    while true do
        queue.tube.mail_msg:put('1')
        fiber.sleep(0.2)
    end
end)
