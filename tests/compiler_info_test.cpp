#include <lls/compiler_info.hpp>

#include <cstdlib>

static_assert(lls::has_cpp23_or_later());
static_assert(!lls::compiler_name().empty());
static_assert(!lls::cpp_standard_name().empty());

int main()
{
    return lls::has_cpp23_or_later() ? EXIT_SUCCESS : EXIT_FAILURE;
}
