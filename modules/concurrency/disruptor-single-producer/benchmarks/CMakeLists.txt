add_executable(
    lls_disruptor_single_producer_benchmark
    throughput.cpp
)
target_link_libraries(
    lls_disruptor_single_producer_benchmark
    PRIVATE lls::disruptor_single_producer
)
lls_apply_project_options(lls_disruptor_single_producer_benchmark)

add_executable(
    lls_disruptor_single_producer_latency_benchmark
    latency.cpp
)
target_link_libraries(
    lls_disruptor_single_producer_latency_benchmark
    PRIVATE lls::disruptor_single_producer
)
lls_apply_project_options(lls_disruptor_single_producer_latency_benchmark)

set_target_properties(
    lls_disruptor_single_producer_benchmark
    lls_disruptor_single_producer_latency_benchmark
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/benchmarks"
)
