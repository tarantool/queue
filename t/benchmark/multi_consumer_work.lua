#!/usr/bin/env tarantool

local clock = require('clock')
local os = require('os')
local fiber = require('fiber')
local queue = require('queue')

-- Set the number of consumers.
local consumers_count = 100
-- Set the number of tasks processed by one consumer per iteration.
local batch_size = 100

local barrier = fiber.cond()
local wait_count = 0

box.cfg()

local test_queue = queue.create_tube('test_queue', 'fifo', {temporary = true})

local function prepare_consumers()
    local consumers = {}
    local test_data = 'test data'

    for i = 1, consumers_count do
        consumers[i] = fiber.create(function()
            wait_count = wait_count + 1
            -- Wait for all consumers to start.
            barrier:wait()

            -- Create test tasks.
            for j = 1, batch_size do
                test_queue:put(test_data)
            end

            wait_count = wait_count + 1
            -- Wait for all consumers to create tasks.
            barrier:wait()

            -- Ack the tasks.
            for j = 1, batch_size do
                local task = test_queue:take()
                test_queue:ack(task[1])
            end

            wait_count = wait_count + 1
        end)
    end

    return consumers
end

local function multi_consumer_bench()
    --- Wait for all consumer fibers.
    local wait_all = function()
        while (wait_count ~= consumers_count) do
            fiber.yield()
        end
        wait_count = 0
    end

    -- Wait for all consumers to start.
    local consumers = prepare_consumers()
    wait_all()

    -- Start timing creation of tasks.
    local start_put_time = clock.proc64()
    barrier:broadcast()
    -- Wait for all consumers to create tasks.
    wait_all()
    -- Complete timing creation of tasks.
    local start_ack_time = clock.proc64()
    barrier:broadcast()
    -- Wait for all tasks to be acked.
    wait_all()
    -- Complete the timing of task confirmation.
    local complete_time = clock.proc64()

    --Print the result in microseconds
    print(string.format("Time it takes to fill the queue: %i",
        tonumber((start_ack_time - start_put_time) / 10^3)))
    print(string.format("Time it takes to confirm the tasks: %i",
        tonumber((complete_time - start_ack_time) / 10^3)))
end

-- Start benchmark.
multi_consumer_bench()

-- Cleanup.
test_queue:drop()

os.exit(0)
