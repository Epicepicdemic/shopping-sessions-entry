FROM alpine:3.23@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2 AS toolchain
RUN apk add --no-cache build-base=0.5-r3 gcc=15.2.0-r2 g++=15.2.0-r2 musl-dev=1.2.5-r24 binutils=2.45.1-r0

FROM toolchain AS allocator
ADD --checksum=sha256:8e0ed89907a681276bff2e49e9a048b47ba51254ab60daf6b3c220acac456a95 \
    https://codeload.github.com/microsoft/mimalloc/tar.gz/refs/tags/v2.2.7 /tmp/mimalloc.tar.gz
RUN mkdir /mimalloc && tar -xzf /tmp/mimalloc.tar.gz --strip-components=1 -C /mimalloc \
    && gcc -O3 -DNDEBUG -march=x86-64-v2 -DMI_STATIC_LIB -DMI_MALLOC_OVERRIDE -DMI_LIBC_MUSL=1 \
       -fno-builtin-malloc -ftls-model=local-dynamic -pthread -I/mimalloc/include -I/mimalloc/src \
       -c /mimalloc/src/static.c -o /mimalloc.o

FROM allocator AS build
WORKDIR /src
COPY proto/src/ .
COPY proto/tools/build-linux.sh .
ARG SHOPPING_CXXFLAGS
RUN sh build-linux.sh /shopping-w5 /mimalloc.o \
    && strip --strip-debug /shopping-w5

FROM scratch
COPY --from=build /shopping-w5 /app/shopping-w5
COPY --from=allocator /mimalloc/LICENSE /licenses/mimalloc.txt
ENV SHOPPING_IO_MODE=pread
USER 1000:1000
ENTRYPOINT ["/app/shopping-w5"]
