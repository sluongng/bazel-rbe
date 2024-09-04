FROM gcr.io/flame-public/rbe-ubuntu24-04:latest

RUN apt update && \
    apt install -y openjdk-25-jdk clang-20 && \
    ln -sf /usr/bin/clang-20 /usr/bin/clang && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
