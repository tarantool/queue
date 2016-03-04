cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(queue)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)

add_subdirectory(queue)

enable_testing()

add_test(000-init.t tarantool ${CMAKE_SOURCE_DIR}/t/000-init.t)
add_test(010-fifo.t tarantool ${CMAKE_SOURCE_DIR}/t/010-fifo.t)
add_test(020-fifottl.t tarantool ${CMAKE_SOURCE_DIR}/t/020-fifottl.t)
add_test(030-utube.t tarantool ${CMAKE_SOURCE_DIR}/t/030-utube.t)
add_test(040-utubettl.t tarantool ${CMAKE_SOURCE_DIR}/t/040-utubettl.t)
add_test(050-ttl.t tarantool ${CMAKE_SOURCE_DIR}/t/050-ttl.t)
add_test(060-async.t tarantool ${CMAKE_SOURCE_DIR}/t/060-async.t)

add_custom_target(test
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMAND ctest)
