FROM alpine:3.23@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2 AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY proto/src/ .
RUN g++ -std=c++20 -O3 -DNDEBUG -march=x86-64-v2 -mtune=generic -flto -pthread -static -s \
    main.cpp -o /shopping-w4

FROM scratch
COPY --from=build /shopping-w4 /app/shopping-w4
ENV W4_IO=pread
USER 1000:1000
ENTRYPOINT ["/app/shopping-w4"]
