find_package(Threads REQUIRED)

add_executable(
    lls_disruptor_paired_cpp
    cpp/paired_benchmark.cpp
)
add_executable(
    lls_disruptor_paired_latency_cpp
    cpp/paired_latency_benchmark.cpp
)
target_link_libraries(
    lls_disruptor_paired_cpp
    PRIVATE
        lls::disruptor_single_producer
        Threads::Threads
)
target_link_libraries(
    lls_disruptor_paired_latency_cpp
    PRIVATE
        lls::disruptor_single_producer
        Threads::Threads
)
lls_apply_project_options(lls_disruptor_paired_cpp)
lls_apply_project_options(lls_disruptor_paired_latency_cpp)

set_target_properties(
    lls_disruptor_paired_cpp
    lls_disruptor_paired_latency_cpp
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/benchmarks/comparisons/disruptor"
)
