#include "cpu.hpp"

extern "C" int shopping_v2(int argc, char** argv);
extern "C" int shopping_v3(int argc, char** argv);
extern "C" int shopping_v4(int argc, char** argv);

int main(int argc, char** argv) {
    using namespace shopping_cpu;
    if (enabled("SHOPPING_BASELINE") || enabled("SHOPPING_SCALAR") || enabled("W4_SCALAR") || enabled("W3_SCALAR"))
        return shopping_v2(argc, argv);
    const auto features = x86_features();
    if (features.v4 && !enabled("SHOPPING_AVX2") && !enabled("W4_AVX2"))
        return shopping_v4(argc, argv);
    if (features.v3)
        return shopping_v3(argc, argv);
    return shopping_v2(argc, argv);
}
