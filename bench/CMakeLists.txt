find_package(benchmark CONFIG QUIET)
if(NOT benchmark_FOUND)
    include(FetchContent)

    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)

    FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.8.4
    )
    FetchContent_MakeAvailable(googlebenchmark)
endif()

add_executable(shared_kv_single_thread_bench
    shared_kv_single_thread_bench.cpp
)
target_link_libraries(shared_kv_single_thread_bench PRIVATE
    dalahash_lib
    benchmark::benchmark
)

add_executable(shared_kv_multi_thread_bench
    shared_kv_multi_thread_bench.cpp
)
target_link_libraries(shared_kv_multi_thread_bench PRIVATE
    dalahash_lib
    benchmark::benchmark
)
