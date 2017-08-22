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

set (LUA_PATH "LUA_PATH=${PROJECT_SOURCE_DIR}/?.lua\\;${PROJECT_SOURCE_DIR}/?/init.lua\\;")

file(GLOB TEST_LIST ${CMAKE_SOURCE_DIR}/t/*.t)

foreach(TEST ${TEST_LIST})
    get_filename_component(TEST_NAME ${TEST} NAME)
    add_test            (memtx-${TEST_NAME} tarantool ${TEST})
    set_tests_properties(memtx-${TEST_NAME} PROPERTIES ENVIRONMENT "${LUA_PATH}")
    set_tests_properties(memtx-${TEST_NAME} PROPERTIES ENVIRONMENT "ENGINE=memtx")
    add_test            (vinyl-${TEST_NAME} tarantool ${TEST})
    set_tests_properties(vinyl-${TEST_NAME} PROPERTIES ENVIRONMENT "${LUA_PATH}")
    set_tests_properties(vinyl-${TEST_NAME} PROPERTIES ENVIRONMENT "ENGINE=vinyl")
endforeach()

add_custom_target(check
    WORKING_DIRECTORY ${PROJECT_BUILD_DIR}
    COMMAND ctest -V)
