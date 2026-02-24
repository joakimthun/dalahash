# FetchContent: downloads and builds dependencies at configure time
include(FetchContent)

# Download GoogleTest v1.15.2 from GitHub
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
)
# Make gtest targets (GTest::gtest_main, etc.) available
FetchContent_MakeAvailable(googletest)

# Unit tests — RESP parser, command dispatch, smoke test
add_executable(dalahash_tests
    smoke_test.cpp
    redis/resp_test.cpp
    redis/command_test.cpp
)
# GTest::gtest_main provides main() so test files only need TEST() macros
target_link_libraries(dalahash_tests dalahash_lib GTest::gtest_main)
target_include_directories(dalahash_tests PRIVATE ${CMAKE_SOURCE_DIR}/src/redis)

# DST tests — deterministic simulation using sim_io_backend.h
add_executable(dst_tests
    net/dst_test.cpp
)
# sim_io_backend.h lives in tests/net/; also needs src/net headers
target_include_directories(dst_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/net
    ${CMAKE_SOURCE_DIR}/src/net
)
target_link_libraries(dst_tests dalahash_lib GTest::gtest_main)

# Real I/O integration tests — starts dalahash and talks over TCP via redis-cli
add_executable(io_uring_integration_tests
    net/io_uring_integration_test.cpp
)
target_link_libraries(io_uring_integration_tests GTest::gtest_main)
target_compile_definitions(io_uring_integration_tests PRIVATE
    DALAHASH_BINARY_PATH="$<TARGET_FILE:dalahash>"
)

# GoogleTest integration — auto-discovers TEST() macros and registers them with CTest
include(GoogleTest)
gtest_discover_tests(dalahash_tests DISCOVERY_MODE PRE_TEST)
gtest_discover_tests(dst_tests DISCOVERY_MODE PRE_TEST)
gtest_discover_tests(io_uring_integration_tests DISCOVERY_MODE PRE_TEST)
