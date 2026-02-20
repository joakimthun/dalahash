# Minimum cmake version required (3.25 for FetchContent improvements)
cmake_minimum_required(VERSION 3.25)

# Define the project name and language (CXX = C++)
project(dalahash LANGUAGES CXX)

# Use C++23 standard and fail if the compiler doesn't support it
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Generate compile_commands.json for editor tooling (clangd, etc.)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Global compiler flags: no exceptions, enable warnings
add_compile_options(-fno-exceptions -Wall -Wextra)

# Find liburing (io_uring wrapper library) — requires: sudo apt install liburing-dev
# find_library: locates the shared/static library file (liburing.so / liburing.a)
# find_path: locates the header directory containing liburing.h
find_library(URING_LIB uring REQUIRED)
find_path(URING_INCLUDE liburing.h REQUIRED)

# Core library — all dalahash logic except main(). Linked by both the
# executable and test targets so tests can access internal functions.
add_library(dalahash_lib STATIC
    src/redis/resp.cpp
    src/redis/command.cpp
    src/net/io_uring_backend.cpp
    src/net/worker.cpp
    src/net/server.cpp
)
# PUBLIC: these include dirs and link libs propagate to anything linking dalahash_lib
target_include_directories(dalahash_lib PUBLIC src ${URING_INCLUDE})
target_link_libraries(dalahash_lib PUBLIC ${URING_LIB} pthread)

# Main executable — just the entry point, links all logic from dalahash_lib
add_executable(dalahash src/main.cpp)
target_link_libraries(dalahash dalahash_lib)

# Enable CTest (cmake's test runner) and pull in the tests/ subdirectory
enable_testing()
add_subdirectory(tests)
