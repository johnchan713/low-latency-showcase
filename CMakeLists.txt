cmake_minimum_required(VERSION 3.28)

project(
    low_latency_showcase
    VERSION 0.1.0
    DESCRIPTION "Measured, documented building blocks for latency-sensitive C++"
    LANGUAGES CXX
)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "Low Latency Showcase currently supports Linux only")
endif()

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")
    message(FATAL_ERROR "Low Latency Showcase currently supports GCC and Clang only")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(CTest)

add_library(lls_headers INTERFACE)
add_library(lls::headers ALIAS lls_headers)

target_include_directories(
    lls_headers
    INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:include>"
)
target_compile_features(lls_headers INTERFACE cxx_std_23)

add_executable(lls_showcase src/main.cpp)
target_link_libraries(lls_showcase PRIVATE lls::headers)
target_compile_definitions(
    lls_showcase
    PRIVATE LLS_PROJECT_VERSION="${PROJECT_VERSION}"
)
lls_apply_project_options(lls_showcase)

if(BUILD_TESTING)
    add_executable(lls_compiler_info_test tests/compiler_info_test.cpp)
    target_link_libraries(lls_compiler_info_test PRIVATE lls::headers)
    lls_apply_project_options(lls_compiler_info_test)

    add_test(NAME lls.smoke COMMAND lls_showcase --self-test)
    add_test(NAME lls.compiler_info COMMAND lls_compiler_info_test)
    set_tests_properties(lls.smoke lls.compiler_info PROPERTIES LABELS "unit")
endif()

# Keep future code visible to configuration and CI through explicit registries.
add_subdirectory(components)
add_subdirectory(snippets)
add_subdirectory(benchmarks)
