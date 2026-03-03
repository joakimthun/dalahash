# Minimum cmake version required (3.25 for FetchContent improvements)
cmake_minimum_required(VERSION 3.25)

# Define the project name and language (CXX = C++)
project(dalahash LANGUAGES CXX)

# Use C++23 standard and fail if the compiler doesn't support it
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Generate compile_commands.json for editor tooling (clangd, etc.)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Protocol selection. Default protocol is Redis.
set(DALAHASH_PROTOCOL "redis" CACHE STRING "Application protocol")
set_property(CACHE DALAHASH_PROTOCOL PROPERTY STRINGS redis echo memcached)

# Global compiler flags: no exceptions, enable warnings
add_compile_options(-fno-exceptions -Wall -Wextra)

# Release performance configuration:
# - CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE: enables LTO/IPO so the optimizer
#   can inline and optimize hot paths across translation units.
# - -march=native and -mtune=native: generate/schedule code for this host CPU.
# - -fuse-ld=lld: use LLVM's linker for reliable and faster LTO linking.
# These are Release-only by design; Debug/sanitizer builds keep normal toolchain behavior.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
add_compile_options($<$<CONFIG:Release>:-march=native> $<$<CONFIG:Release>:-mtune=native>)
# Debug stack traces from ASSERT() use backtrace_symbols_fd(3). `-rdynamic`
# exports non-static symbols to the dynamic symbol table so callstacks resolve
# to function names instead of mostly raw addresses.
add_link_options(
    $<$<CONFIG:Debug>:-rdynamic>
    $<$<CONFIG:Release>:-fuse-ld=lld>
    $<$<CONFIG:Release>:-march=native>
    $<$<CONFIG:Release>:-mtune=native>
)

# AddressSanitizer (ASan) and LeakSanitizer (LSan) build toggles.
# Applied globally because sanitizer runtimes require consistent flags across
# all objects linked into each binary (including GoogleTest via FetchContent).
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_LSAN "Enable LeakSanitizer" OFF)
option(ENABLE_BENCHMARKS "Build benchmark targets" OFF)

set(DALAHASH_SANITIZERS "")
if(ENABLE_ASAN)
    list(APPEND DALAHASH_SANITIZERS address)
endif()
if(ENABLE_LSAN)
    list(APPEND DALAHASH_SANITIZERS leak)
endif()

if(DALAHASH_SANITIZERS)
    list(JOIN DALAHASH_SANITIZERS "," DALAHASH_SANITIZER_FLAGS)
    add_compile_options(-fsanitize=${DALAHASH_SANITIZER_FLAGS} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${DALAHASH_SANITIZER_FLAGS})
endif()

# Find liburing (io_uring wrapper library) — requires: sudo apt install liburing-dev
# Prefer the static archive so dalahash is as statically linked as possible.
# find_path: locates the header directory containing liburing.h
find_library(URING_LIB NAMES liburing.a uring REQUIRED)
find_path(URING_INCLUDE liburing.h REQUIRED)

# Core library — all dalahash logic except main(). Linked by both the
# executable and test targets so tests can access internal functions.
add_library(dalahash_lib STATIC
    src/base/assert.cpp
    src/kv/shared_kv_store.cpp
    src/redis/resp.cpp
    src/redis/command.cpp
    src/memcached/memcached_parse.cpp
    src/memcached/memcached_response.cpp
    src/memcached/memcached_command.cpp
    src/net/io_uring_backend.cpp
    src/net/worker.cpp
    src/net/server.cpp
)
# PUBLIC: these include dirs and link libs propagate to anything linking dalahash_lib
target_include_directories(dalahash_lib PUBLIC src ${URING_INCLUDE})
target_link_libraries(dalahash_lib PUBLIC ${URING_LIB} pthread)
target_compile_definitions(dalahash_lib PUBLIC
    $<$<CONFIG:Debug>:DALAHASH_DEBUG_ASSERTS=1>
)

if(DALAHASH_PROTOCOL STREQUAL "redis")
    target_compile_definitions(dalahash_lib PUBLIC DALAHASH_PROTOCOL_REDIS=1)
elseif(DALAHASH_PROTOCOL STREQUAL "echo")
    target_compile_definitions(dalahash_lib PUBLIC DALAHASH_PROTOCOL_ECHO=1)
elseif(DALAHASH_PROTOCOL STREQUAL "memcached")
    target_compile_definitions(dalahash_lib PUBLIC DALAHASH_PROTOCOL_MEMCACHED=1)
else()
    message(FATAL_ERROR "Unsupported DALAHASH_PROTOCOL: ${DALAHASH_PROTOCOL}")
endif()

# Main executable — just the entry point, links all logic from dalahash_lib
add_executable(dalahash src/main.cpp)
target_link_libraries(dalahash dalahash_lib)
# Statically link C++ runtime pieces where possible while keeping glibc dynamic.
target_link_options(dalahash PRIVATE -static-libstdc++ -static-libgcc)

# Enable CTest (cmake's test runner) and pull in the tests/ subdirectory
enable_testing()
add_subdirectory(tests)

if(ENABLE_BENCHMARKS)
    add_subdirectory(bench)
endif()

# Register an end-to-end benchmark wrapper target for Redis and Memcached protocols.
if(DALAHASH_PROTOCOL STREQUAL "redis")
    add_custom_target(e2e_benchmark
        COMMAND ${CMAKE_SOURCE_DIR}/bench/run_benchmark.sh --binary $<TARGET_FILE:dalahash>
        DEPENDS dalahash
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running end-to-end benchmark with memtier_benchmark"
        VERBATIM
    )
elseif(DALAHASH_PROTOCOL STREQUAL "memcached")
    add_custom_target(e2e_benchmark
        COMMAND ${CMAKE_SOURCE_DIR}/bench/run_benchmark.sh --memcached --binary $<TARGET_FILE:dalahash>
        DEPENDS dalahash
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running end-to-end memcached benchmark with memtier_benchmark"
        VERBATIM
    )
else()
    add_custom_target(e2e_benchmark
        COMMAND ${CMAKE_COMMAND} -E echo "ERROR: e2e_benchmark requires DALAHASH_PROTOCOL=redis or memcached (current: ${DALAHASH_PROTOCOL})"
        COMMAND ${CMAKE_COMMAND} -E false
        COMMENT "Benchmark requires Redis or Memcached protocol build"
    )
endif()

find_program(DALAHASH_CLANG_FORMAT NAMES clang-format-21 clang-format)
if(DALAHASH_CLANG_FORMAT)
    file(GLOB_RECURSE DALAHASH_FORMAT_FILES CONFIGURE_DEPENDS
        "${CMAKE_SOURCE_DIR}/src/*.c"
        "${CMAKE_SOURCE_DIR}/src/*.cc"
        "${CMAKE_SOURCE_DIR}/src/*.cpp"
        "${CMAKE_SOURCE_DIR}/src/*.cxx"
        "${CMAKE_SOURCE_DIR}/src/*.h"
        "${CMAKE_SOURCE_DIR}/src/*.hh"
        "${CMAKE_SOURCE_DIR}/src/*.hpp"
        "${CMAKE_SOURCE_DIR}/tests/*.c"
        "${CMAKE_SOURCE_DIR}/tests/*.cc"
        "${CMAKE_SOURCE_DIR}/tests/*.cpp"
        "${CMAKE_SOURCE_DIR}/tests/*.cxx"
        "${CMAKE_SOURCE_DIR}/tests/*.h"
        "${CMAKE_SOURCE_DIR}/tests/*.hh"
        "${CMAKE_SOURCE_DIR}/tests/*.hpp"
        "${CMAKE_SOURCE_DIR}/bench/*.c"
        "${CMAKE_SOURCE_DIR}/bench/*.cc"
        "${CMAKE_SOURCE_DIR}/bench/*.cpp"
        "${CMAKE_SOURCE_DIR}/bench/*.cxx"
        "${CMAKE_SOURCE_DIR}/bench/*.h"
        "${CMAKE_SOURCE_DIR}/bench/*.hh"
        "${CMAKE_SOURCE_DIR}/bench/*.hpp"
    )
    add_custom_target(format
        COMMAND "${DALAHASH_CLANG_FORMAT}" -i -style=file ${DALAHASH_FORMAT_FILES}
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        COMMENT "Formatting dalahash C/C++ sources"
        VERBATIM
    )
else()
    message(STATUS "clang-format not found; 'format' target will not be generated")
endif()
