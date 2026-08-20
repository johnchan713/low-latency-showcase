add_library(lls_spin_wait INTERFACE)
add_library(lls::spin_wait ALIAS lls_spin_wait)

target_include_directories(
    lls_spin_wait
    INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_compile_features(lls_spin_wait INTERFACE cxx_std_23)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
