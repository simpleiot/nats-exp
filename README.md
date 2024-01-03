# NATS Experiments

Various experiments of using NATS Jetstream as a primary store (instead of SQLite).

- using streams as a point store: [point-store](point-store)
- using streams as a time-series data store: [tsd](tsd)
  - this experiment focuses on the performance of a stream
- syncing streams between a hub and leaf instances: [bi-directional-sync](bi-directional-sync)
  - in this experiment, we stop a leaf node, then restart it and verify data written while down is synchronized.
