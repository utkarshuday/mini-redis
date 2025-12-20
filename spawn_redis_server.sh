exec cargo run \
--bin mini-redis-server \
--quiet \
--release \
--manifest-path $(dirname $0)/Cargo.toml \
-- "$@"
