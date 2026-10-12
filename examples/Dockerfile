FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 ca-certificates libssl3t64 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY tkfs .
ENTRYPOINT ["./tkfs"]
