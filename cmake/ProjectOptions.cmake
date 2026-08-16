include_guard(GLOBAL)

include(LowLatencyBuild)
include(Sanitizers)

option(LLS_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
option(LLS_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(LLS_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(LLS_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(LLS_ENABLE_LTO "Enable interprocedural optimization" OFF)
option(LLS_NATIVE_ARCH "Tune generated code for the build host" OFF)

function(lls_apply_project_options target_name)
    target_compile_features(${target_name} PRIVATE cxx_std_23)
    set_target_properties(
        ${target_name}
        PROPERTIES
            CXX_EXTENSIONS OFF
    )

    if(MSVC)
        target_compile_options(${target_name} PRIVATE /W4 /permissive-)
        if(LLS_WARNINGS_AS_ERRORS)
            target_compile_options(${target_name} PRIVATE /WX)
        endif()
    else()
        target_compile_options(
            ${target_name}
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
                -Wconversion
                -Wsign-conversion
                -Wshadow
        )
        if(LLS_WARNINGS_AS_ERRORS)
            target_compile_options(${target_name} PRIVATE -Werror)
        endif()
    endif()

    lls_enable_sanitizers(${target_name})
    lls_enable_low_latency_build(${target_name})
endfunction()
