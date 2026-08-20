add_executable(lls_wait_strategy_benchmark wait_strategy.cpp)
target_link_libraries(
    lls_wait_strategy_benchmark
    PRIVATE lls::disruptor_single_producer lls::spin_wait
)
lls_apply_project_options(lls_wait_strategy_benchmark)

set_target_properties(
    lls_wait_strategy_benchmark
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/benchmarks"
)
