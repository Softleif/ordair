# ordair

A parallel map on a [rayon] pool whose results reach a sequential consumer in
input order, with a hard bound on how far the workers run ahead of it.

```rust
ordair::in_order(&regions)                // any IntoIterator; borrowed data is fine
    .pool(&pool)                          // optional: names, handlers, size
    .window(32)                           // optional: how far workers may run ahead
    .map_init(
        || open_readers(),                // once per worker, on its thread, fallible
        |readers, region| process(readers, region),
    )
    .try_for_each(|records| writer.write(records))?; // on the calling thread, in order
```

Without per-worker state, use `.map(process)` instead. By default it
runs on the current rayon pool with a window of four results per thread.

- **Order by construction.** Each item's result gets a one-shot channel whose
  receiver is queued when the item is taken, in a bounded queue. No indices,
  no reorder buffer.
- **The bound counts what matters.** A result that is finished but waiting
  behind a slow item still occupies the window, so memory stays bounded however
  uneven the items are. Reorder-buffer designs (`ordered-channel`, `ordq`)
  bound only their channel and let finished results pile up behind a slow head.
- **Per-worker state** is built on the worker's own thread when it takes its
  first item, so it need not be `Send` or `Clone`, building it may fail, and a
  thread that gets no item builds none.
- **Errors, not panics.** An `init` error, a worker panic or a `consume` error
  stops the workers from taking new items and comes back once the in-flight
  items are done: `Error::Worker(Init | Panicked)`, `Error::Consumer`, or
  `Error::Both` when a worker and the consumer failed independently, so
  neither is lost. A panic in `consume` unwinds out of the call. It never
  hangs, including when every worker panics.
- **Runs on your pool**, or the current one. Every thread of the pool works on
  the call until the items run out; calling it from one of the pool's own
  threads works too.

The only dependency is `rayon`.

## Alternatives

| crate | why not |
| --- | --- |
| `pariter` | closest maintained one; no per-worker init, a worker panic panics the consumer (found by polling every 100 µs), spawns its own threads |
| `pipeliner` (`ordered_map`) | same algorithm, but `'static` only, no per-worker state, panics; last release 2020 |
| `plmap` | window fixed at workers + 1, clone-per-worker state, a worker panic can abort the process (panic in `Drop` while unwinding) |
| `ordq`, `ordered-channel` | finished results are buffered without bound behind a slow item |
| `rayon` | no ordered streaming ([rayon#210](https://github.com/rayon-rs/rayon/issues/210)); `map_init` runs per split, not per thread |

[rayon]: https://docs.rs/rayon

## Development

```sh
cargo nextest run          # tests; the property test runs 2000 cases
cargo test --doc           # nextest skips doctests
HEGEL_DEFAULT_PROFILE=thorough cargo nextest run --release   # 20000 cases
```

CI runs these on Linux (x86-64 and arm64), the deeper profile nightly
with a new seed, and `cargo deny` daily.

## License

MIT OR Apache-2.0
