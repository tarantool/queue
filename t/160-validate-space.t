#!/usr/bin/env tarantool

local tap = require('tap')
local os = require('os')
local tnt  = require('t.tnt')
local test = tap.test('test space validation')

local fifo = require('queue.abstract.driver.fifo')
local fifottl = require('queue.abstract.driver.fifottl')
local utube = require('queue.abstract.driver.utube')
local utubettl = require('queue.abstract.driver.utubettl')

local engine = os.getenv('ENGINE') or 'memtx'

test:plan(8)
tnt.cfg{}

local function test_corrupted_space(test, driver, indexes)
    test:plan(table.getn(indexes))

    -- Can't drop primary key in space while secondary keys exist.
    -- So, drop other indexes previously.
    local function remove_task_id_index(space, indexes)
        for _, index in pairs(indexes) do
            if index ~= 'task_id' then
                space.index[index]:drop()
            end
        end
        space.index.task_id:drop()
    end

    for _, index in pairs(indexes) do
        test:test(index .. ' index does not exist', function(test)
            test:plan(2)

            local space = driver.create_space('corrupted_space',
                {engine = engine})

            if index == 'task_id' then
                remove_task_id_index(space, indexes)
            else
                space.index[index]:drop()
            end

            local res, err = pcall(driver.new, space)
            local err_match_msg = string.format('space "corrupted_space"' ..
                ' does not have "%s" index', index)
            test:ok(not res, 'exception was thrown')
            test:ok(err:match(err_match_msg) ~= nil, 'text of exception')

            space:drop()
        end)
    end
end

local function test_name_conflict(test, driver)
    test:plan(2)

    local conflict_space = box.schema.create_space('conflict_tube')
    local res, err = pcall(driver.create_space,'conflict_tube',
        {engine = engine, if_not_exists = true})

    test:ok(not res, 'exception was thrown')
    test:ok(err:match('space "conflict_tube" does not' ..
            ' have "task_id" index') ~= nil, 'text of exception')

    conflict_space:drop()
end

test:test('test corrupted space fifo', function(test)
    test_corrupted_space(test, fifo, {'task_id', 'status'})
end)

test:test('test corrupted space fifottl', function(test)
    test_corrupted_space(test, fifottl, {'task_id', 'status', 'watch'})
end)

test:test('test corrupted space utube', function(test)
    test_corrupted_space(test, utube, {'task_id', 'status', 'utube'})
end)

test:test('test corrupted space utubettl', function(test)
    test_corrupted_space(test, utubettl,
        {'task_id', 'status', 'utube', 'watch', 'utube_pri'})
end)

test:test('Space name conflict fifo', function(test)
    test_name_conflict(test, fifo)
end)

test:test('Space name conflict fifo', function(test)
    local fifo = require('queue.abstract.driver.fifo')
    test_name_conflict(test, fifottl)
end)

test:test('Space name conflict fifo', function(test)
    local fifo = require('queue.abstract.driver.fifo')
    test_name_conflict(test, utube)
end)

test:test('Space name conflict fifo', function(test)
    local fifo = require('queue.abstract.driver.fifo')
    test_name_conflict(test, utubettl)
end)

tnt.finish()
os.exit(test:check() and 0 or 1)
-- vim: set ft=lua :
