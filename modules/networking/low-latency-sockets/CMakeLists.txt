add_library(lls_low_latency_sockets INTERFACE)
add_library(lls::low_latency_sockets ALIAS lls_low_latency_sockets)

target_include_directories(
    lls_low_latency_sockets
    INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_compile_features(lls_low_latency_sockets INTERFACE cxx_std_23)
target_link_libraries(lls_low_latency_sockets INTERFACE lls::spin_wait)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
