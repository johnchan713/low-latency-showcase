#pragma once

#include <string_view>

namespace lls {

[[nodiscard]] constexpr long cpp_standard_value() noexcept
{
    return __cplusplus;
}

[[nodiscard]] constexpr bool has_cpp23_or_later() noexcept
{
    return cpp_standard_value() > 202002L;
}

[[nodiscard]] constexpr std::string_view cpp_standard_name() noexcept
{
    if constexpr (__cplusplus >= 202302L) {
        return "C++23";
    } else if constexpr (__cplusplus > 202002L) {
        return "C++23 (compiler draft value)";
    } else {
        return "pre-C++23";
    }
}

[[nodiscard]] constexpr std::string_view compiler_name() noexcept
{
#if defined(__clang__)
    return "Clang " __clang_version__;
#elif defined(__GNUC__)
    return "GCC " __VERSION__;
#else
    return "unknown compiler";
#endif
}

} // namespace lls
