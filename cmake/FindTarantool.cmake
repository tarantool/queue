# Define GNU standard installation directories
include(GNUInstallDirs)

macro(extract_definition name output input)
    string(REGEX MATCH "#define[\t ]+${name}[\t ]+\"([^\"]*)\""
        _t "${input}")
    string(REGEX REPLACE "#define[\t ]+${name}[\t ]+\"(.*)\"" "\\1"
        ${output} "${_t}")
endmacro()

find_path(TARANTOOL_INCLUDE_DIR tarantool/module.h
  HINTS ENV TARANTOOL_DIR
)

if(TARANTOOL_INCLUDE_DIR)
    set(_config "-")
    file(READ "${TARANTOOL_INCLUDE_DIR}/tarantool/module.h" _config0)
    string(REPLACE "\\" "\\\\" _config ${_config0})
    unset(_config0)
    extract_definition(PACKAGE_VERSION TARANTOOL_VERSION ${_config})
    extract_definition(INSTALL_PREFIX _install_prefix ${_config})
    unset(_config)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TARANTOOL
    REQUIRED_VARS TARANTOOL_INCLUDE_DIR VERSION_VAR TARANTOOL_VERSION)
if(TARANTOOL_FOUND)
    set(TARANTOOL_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}/tarantool")
    set(TARANTOOL_INSTALL_LUADIR "${CMAKE_INSTALL_DATADIR}/tarantool")
    set(TARANTOOL_INCLUDE_DIRS "${TARANTOOL_INCLUDE_DIR}"
                               "${TARANTOOL_INCLUDE_DIR}/tarantool/")

    if (NOT "${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr/local" AND
            NOT "${CMAKE_INSTALL_PREFIX}" STREQUAL "${_install_prefix}")
        message(WARNING "Provided CMAKE_INSTALL_PREFIX is different from "
            "CMAKE_INSTALL_PREFIX of Tarantool. You might need to set "
            "corrent package.path/package.cpath to load this module or "
            "change your build prefix:"
            "\n"
            "cmake . -DCMAKE_INSTALL_PREFIX=${_install_prefix}"
            "\n"
        )
    endif ()
    if (NOT TARANTOOL_FIND_QUIETLY AND NOT FIND_TARANTOOL_DETAILS)
        set(FIND_TARANTOOL_DETAILS ON CACHE INTERNAL "Details about TARANTOOL")
        message(STATUS "Tarantool LUADIR is ${TARANTOOL_INSTALL_LUADIR}")
        message(STATUS "Tarantool LIBDIR is ${TARANTOOL_INSTALL_LIBDIR}")
    endif ()
endif()
mark_as_advanced(TARANTOOL_INCLUDE_DIRS TARANTOOL_INSTALL_LIBDIR
    TARANTOOL_INSTALL_LUADIR)
