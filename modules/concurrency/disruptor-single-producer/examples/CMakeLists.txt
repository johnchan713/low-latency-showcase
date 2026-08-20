add_executable(
    lls_disruptor_single_producer_example
    basic.cpp
)
target_link_libraries(
    lls_disruptor_single_producer_example
    PRIVATE lls::disruptor_single_producer
)
lls_apply_project_options(lls_disruptor_single_producer_example)
