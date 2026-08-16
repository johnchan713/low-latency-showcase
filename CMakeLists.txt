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

option(LLS_BUILD_EXAMPLES "Build example executables" ON)
option(LLS_BUILD_BENCHMARKS "Build benchmark executables" OFF)

add_library(lls_headers INTERFACE)
add_library(lls::headers ALIAS lls_headers)

target_include_directories(
    lls_headers
    INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:include>"
)
target_compile_features(lls_headers INTERFACE cxx_std_23)

# Every directory below is an explicit registry. New code is never discovered
# through source globbing, so review and CI scope remain visible.
add_subdirectory(support)
add_subdirectory(modules)
add_subdirectory(apps)

if(LLS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(LLS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
