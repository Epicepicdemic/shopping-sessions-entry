#!/bin/sh
set -eu

compiler=${CXX:-g++}
flags=${SHOPPING_CXXFLAGS:--O3 -DNDEBUG -mtune=generic -flto=auto -fno-rtti}
output=$1
shift

build_variant() {
    level=$1
    shift
    # The three translation units have separate namespaces and entry points.
    # Their instruction sets are selected once by the baseline dispatcher.
    "$compiler" -std=c++20 $flags -pthread -march="x86-64-$level" "$@" \
        -DSHOPPING_NAMESPACE="shopping_${level}_impl" -DSHOPPING_ENTRY="shopping_${level}" \
        "-DSHOPPING_ISA_NAME=\"x86-64-$level\"" -c main.cpp -o "engine-$level.o"
}

build_variant v2 &
v2_pid=$!
build_variant v3 -mpclmul &
v3_pid=$!
build_variant v4 -mpclmul &
v4_pid=$!
wait "$v2_pid"
wait "$v3_pid"
wait "$v4_pid"

"$compiler" -std=c++20 $flags -pthread -march=x86-64-v2 -c dispatch.cpp -o dispatch.o
# A static allocator override, when supplied, must precede the CRT archives.
"$compiler" $flags -march=x86-64-v2 -pthread -static "$@" dispatch.o \
    engine-v2.o engine-v3.o engine-v4.o -o "$output"
