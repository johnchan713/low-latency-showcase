add_executable(lls_showcase main.cpp)
target_link_libraries(lls_showcase PRIVATE lls::headers)
target_compile_definitions(
    lls_showcase
    PRIVATE LLS_PROJECT_VERSION="${PROJECT_VERSION}"
)
lls_apply_project_options(lls_showcase)

# Preserve the existing command path even though the source now has an owner.
set_target_properties(
    lls_showcase
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
)

if(BUILD_TESTING)
    add_executable(
        lls_compiler_info_test
        tests/compiler_info_test.cpp
    )
    target_link_libraries(lls_compiler_info_test PRIVATE lls::headers)
    lls_apply_project_options(lls_compiler_info_test)

    add_test(NAME lls.smoke COMMAND lls_showcase --self-test)
    add_test(NAME lls.compiler_info COMMAND lls_compiler_info_test)
    set_tests_properties(
        lls.smoke
        lls.compiler_info
        PROPERTIES LABELS "unit"
    )
endif()
