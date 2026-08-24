find_package(Threads REQUIRED)

add_executable(lls_socket_latency_benchmark socket_latency.cpp)
target_link_libraries(
    lls_socket_latency_benchmark
    PRIVATE
        Threads::Threads
        lls::spin_wait
)
lls_apply_project_options(lls_socket_latency_benchmark)

set_target_properties(
    lls_socket_latency_benchmark
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/benchmarks"
)

if(BUILD_TESTING)
    add_test(
        NAME lls.socket_latency_smoke
        COMMAND lls_socket_latency_benchmark --self-test
    )
    add_test(
        NAME lls.socket_latency_tcp_spin_smoke
        COMMAND
            lls_socket_latency_benchmark
            --self-test
            --profile tcp-spin
            --protocol tcp
    )
    add_test(
        NAME lls.socket_latency_udp_spin_smoke
        COMMAND
            lls_socket_latency_benchmark
            --self-test
            --profile udp-connected-spin
            --protocol udp
    )
    set_tests_properties(
        lls.socket_latency_smoke
        lls.socket_latency_tcp_spin_smoke
        lls.socket_latency_udp_spin_smoke
        PROPERTIES
            LABELS "integration;networking"
            TIMEOUT 30
    )
endif()
