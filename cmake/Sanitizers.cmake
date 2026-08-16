include_guard(GLOBAL)

function(lls_enable_sanitizers target_name)
    if(LLS_ENABLE_ASAN AND LLS_ENABLE_TSAN)
        message(FATAL_ERROR "AddressSanitizer and ThreadSanitizer cannot be combined")
    endif()

    if(NOT LLS_ENABLE_ASAN AND NOT LLS_ENABLE_UBSAN AND NOT LLS_ENABLE_TSAN)
        return()
    endif()

    if(MSVC)
        message(FATAL_ERROR "The selected sanitizer preset is not configured for MSVC")
    endif()

    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        message(FATAL_ERROR "Sanitizers require a supported Clang or GCC toolchain")
    endif()

    set(enabled_sanitizers "")
    if(LLS_ENABLE_ASAN)
        list(APPEND enabled_sanitizers address)
    endif()
    if(LLS_ENABLE_UBSAN)
        list(APPEND enabled_sanitizers undefined)
    endif()
    if(LLS_ENABLE_TSAN)
        list(APPEND enabled_sanitizers thread)
    endif()

    list(JOIN enabled_sanitizers "," sanitizer_list)
    target_compile_options(
        ${target_name}
        PRIVATE
            "-fsanitize=${sanitizer_list}"
            -fno-omit-frame-pointer
    )
    target_link_options(${target_name} PRIVATE "-fsanitize=${sanitizer_list}")
endfunction()
