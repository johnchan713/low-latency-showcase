add_executable(lls_spin_wait_test spin_wait_test.cpp)
target_link_libraries(lls_spin_wait_test PRIVATE lls::spin_wait)
if(COMMAND lls_apply_project_options)
    lls_apply_project_options(lls_spin_wait_test)
else()
    target_compile_features(lls_spin_wait_test PRIVATE cxx_std_23)
    set_target_properties(
        lls_spin_wait_test
        PROPERTIES CXX_EXTENSIONS OFF
    )
endif()

add_test(NAME lls.spin_wait COMMAND lls_spin_wait_test)
set_tests_properties(lls.spin_wait PROPERTIES LABELS "unit;concurrency")
