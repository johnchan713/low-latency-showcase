add_executable(lls_spin_wait_test spin_wait_test.cpp)
target_link_libraries(lls_spin_wait_test PRIVATE lls::spin_wait)
lls_apply_project_options(lls_spin_wait_test)

add_test(NAME lls.spin_wait COMMAND lls_spin_wait_test)
set_tests_properties(lls.spin_wait PROPERTIES LABELS "unit;concurrency")
