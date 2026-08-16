if(BUILD_TESTING)
    add_subdirectory(testing)
endif()

if(LLS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarking)
endif()
