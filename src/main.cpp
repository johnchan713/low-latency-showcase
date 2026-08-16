#include <lls/compiler_info.hpp>

#include <cstdlib>
#include <iostream>
#include <string_view>

#ifndef LLS_PROJECT_VERSION
#define LLS_PROJECT_VERSION "development"
#endif

namespace {

constexpr std::string_view usage =
    "Usage: lls_showcase [--help | --self-test]\n";

int run_self_test()
{
    if (!lls::has_cpp23_or_later()) {
        std::cerr << "FAIL: the executable was not built in C++23 mode\n";
        return EXIT_FAILURE;
    }

    std::cout << "PASS: C++23 compiler mode detected\n";
    return EXIT_SUCCESS;
}

void print_summary()
{
    std::cout << "Low Latency Showcase " << LLS_PROJECT_VERSION << '\n'
              << "Compiler: " << lls::compiler_name() << '\n'
              << "Language mode: " << lls::cpp_standard_name() << " ("
              << lls::cpp_standard_value() << ")\n"
              << "\n"
              << "Every technique in this project must be measured, reproducible, "
                 "and documented with its trade-offs.\n";
}

} // namespace

int main(int argc, char* argv[])
{
    if (argc == 1) {
        print_summary();
        return EXIT_SUCCESS;
    }

    if (argc == 2) {
        const std::string_view argument{argv[1]};
        if (argument == "--help") {
            std::cout << usage;
            return EXIT_SUCCESS;
        }
        if (argument == "--self-test") {
            return run_self_test();
        }
    }

    std::cerr << usage;
    return EXIT_FAILURE;
}
