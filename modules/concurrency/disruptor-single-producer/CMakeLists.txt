add_library(lls_disruptor_single_producer INTERFACE)
add_library(
    lls::disruptor_single_producer
    ALIAS lls_disruptor_single_producer
)

target_include_directories(
    lls_disruptor_single_producer
    INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_compile_features(
    lls_disruptor_single_producer
    INTERFACE cxx_std_23
)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(LLS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(LLS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
