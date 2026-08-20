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

add_executable(
    lls_disruptor_single_producer_configuration_sweep
    configuration_sweep.cpp
)
target_link_libraries(
    lls_disruptor_single_producer_configuration_sweep
    PRIVATE lls::disruptor_single_producer
)
lls_apply_project_options(lls_disruptor_single_producer_configuration_sweep)

set_target_properties(
    lls_disruptor_single_producer_benchmark
    lls_disruptor_single_producer_latency_benchmark
    lls_disruptor_single_producer_configuration_sweep
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/benchmarks"
)
