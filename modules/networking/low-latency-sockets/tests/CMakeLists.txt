find_package(Threads REQUIRED)

add_executable(
    lls_low_latency_sockets_test
    low_latency_sockets_test.cpp
)
target_link_libraries(
    lls_low_latency_sockets_test
    PRIVATE
        lls::low_latency_sockets
        Threads::Threads
)
if(COMMAND lls_apply_project_options)
    lls_apply_project_options(lls_low_latency_sockets_test)
else()
    target_compile_features(lls_low_latency_sockets_test PRIVATE cxx_std_23)
    set_target_properties(
        lls_low_latency_sockets_test
        PROPERTIES CXX_EXTENSIONS OFF
    )
endif()

add_test(
    NAME lls.low_latency_sockets
    COMMAND lls_low_latency_sockets_test
)
set_tests_properties(
    lls.low_latency_sockets
    PROPERTIES
        LABELS "integration;networking"
        TIMEOUT 30
)
