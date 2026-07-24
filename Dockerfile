FROM rust:1-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --break-system-packages dora-rs==0.5.0 pyarrow numpy opencv-python-headless
RUN ARCH=$(uname -m) \
    && curl -fsSL "https://github.com/dora-rs/dora/releases/download/v0.5.0/dora-cli-${ARCH}-unknown-linux-gnu.tar.gz" \
    | tar xz -C /tmp \
    && mv "/tmp/dora-cli-${ARCH}-unknown-linux-gnu/dora" /usr/local/bin/dora \
    && rm -rf "/tmp/dora-cli-${ARCH}-unknown-linux-gnu" && dora --version

WORKDIR /bagflow
COPY Cargo.toml ./
COPY crates ./crates
COPY python ./python
RUN cargo build --release && cp target/release/bagflow target/release/bagflow-source /usr/local/bin/
COPY examples ./examples
