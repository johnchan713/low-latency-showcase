add_executable(
    lls_disruptor_single_producer_test
    disruptor_single_producer_test.cpp
)
target_link_libraries(
    lls_disruptor_single_producer_test
    PRIVATE lls::disruptor_single_producer
)
lls_apply_project_options(lls_disruptor_single_producer_test)

add_test(
    NAME lls.disruptor_single_producer
    COMMAND lls_disruptor_single_producer_test
)
set_tests_properties(
    lls.disruptor_single_producer
    PROPERTIES LABELS "unit;concurrency"
)
