# ordair

A parallel map on a [rayon] pool, consumed in input order.

Workers run ahead of the consumer, but only by a fixed number of results, so
memory stays bounded.

```rust
ordair::in_order(&regions)        // any IntoIterator; borrowing is fine
    .pool(&pool)                  // optional; default: the current rayon pool
    .window(32)                   // optional; default: 4 per thread
    .map_init(
        || open_readers(),        // once per worker thread; may fail
        |readers, region| process(readers, region),
    )
    // Runs on the calling thread, in input order.
    // `??`: the outer `?` is a panic, the inner one is your own error.
    .try_for_each(|records| writer.write(records))??;
```

No per-worker state? Use `.map(process)` instead of `.map_init`.

Want an `Iterator` instead, for `zip`, `take`, a `for` loop with `break`?
`with_iter` lends one to a closure. It cannot leave the closure, so borrowing,
the error model and "it never hangs" all still hold:

```rust
ordair::in_order(&words)
    .map(|w| w.to_uppercase())
    .with_iter(|lines| {
        for line in lines.take(10) {
            writeln!(out, "{line}")?;
        }
        Ok(())
    })??;
```

A worker panic or an `init` error ends the iterator early; the real cause comes
back in the same two-layer result. Dropping the iterator early stops the
workers, like an error from `consume` does.

## Why ordair

- **In order by construction.** No indices, no reorder buffer.
- **Bounded memory.** A finished result waiting behind a slow item still counts
  against the window. Reorder-buffer designs (`ordered-channel`, `ordq`) let
  such results pile up.
- **Per-worker state.** Built on the worker's own thread when it takes its
  first item. It need not be `Send` or `Clone`, and building it may fail.
- **Your pool, shared.** Or the current one. Workers never wait for the
  consumer: when they run out of items to start, they give their threads back.
  So the items, the work and `consume` can all use the same pool: `install`,
  `join`, `broadcast`, a parallel iterator, a library that uses rayon inside,
  another map. Calling it from one of the pool's own threads works too, even
  the only one, and so does chaining maps on one pool.
- **Items need not be `Send`.** They are taken on the calling thread, as the
  window has room. A slow iterator slows the consumer, so put expensive work in
  `map`.
- **One dependency:** `rayon-core`, the pool half of `rayon`.

## Errors

`try_for_each` returns `Result<Result<(), E>, Panic>`, `with_iter`
`Result<Result<T, E>, Panic>`:

- **The inner error is yours,** from `init` or `consume`, untouched. An
  `eyre::Report` keeps its backtrace and span trace. For `with_iter`, `consume`
  is the closure.
- **The outer error is a panic,** in a worker or in `consume`. It is
  `Send + Sync`, so `?` turns it into an `anyhow` or `eyre` error. To panic
  instead, call `.or_unwind()`.

Either way, the workers stop taking new items and the call returns once the
items in flight are done. It never hangs.

**When `process` can fail,** return a `Result` from it and pass the error on
with `?` in `consume`. `init` and `consume` share one error type. That is easy
with `anyhow` or `eyre`; with concrete types, one of them may need a `map_err`.

## How it works

The consumer takes the items, on the calling thread, while the window has room.
Each item gets a one-shot channel for its result; the consumer keeps the
receiving ends in a queue, so the results come out in input order.

Workers take the items the consumer took. When there are none left, a worker
returns its thread to the pool, and the consumer starts a new one when it takes
more. A worker that leaves items behind starts another, up to one per thread.
The state `init` builds stays behind on its thread for whichever worker runs
there next, and is dropped there at the end. A consumer on a thread of the pool
runs the pool's jobs while it waits, as rayon's `join` does.

## Alternatives

| crate | features | missing | bonus |
| --- | --- | --- | --- |
| [`pariter`] | ordered, window counts finished results, borrows (scoped) | per-worker init; a worker panic panics the consumer (found by polling every 100 µs); spawns its own threads | iterator adapter (drop-in for `.map`), `parallel_filter`, `readahead`, profiling |
| [`pipeliner`] (`ordered_map`) | same algorithm | borrowing (`'static` only), per-worker state, errors (panics); last release 2020 | unordered `map`, returns an iterator, chainable stages |
| [`plmap`] | ordered, borrows (scoped) | window fixed at workers + 1; state is cloned per worker; a worker panic can abort the process (panic in `Drop` while unwinding) | returns an iterator, `Mapper` trait |
| [`ordq`] | ordered, per-worker state (one `Work` value each) | bound on finished results (unbounded result channel); borrowing (`'static`) | push-based: send jobs from anywhere, per-worker stats |
| [`ordered-channel`] | ordered channel | bound on finished results (reorder heap grows behind a slow item); you bring the workers | any producer, not tied to an iterator or pool |
| [`rayon-par-bridge`] | rayon pool, bounded channel to a sequential iterator | order; per-worker state | takes any `ParallelIterator`, so all of rayon's adapters |
| [`rayon`] | rayon pool, borrows | ordered streaming ([rayon#210](https://github.com/rayon-rs/rayon/issues/210)); `map_init` runs per split, not per thread | everything else rayon does |

[`pariter`]: https://crates.io/crates/pariter
[`pipeliner`]: https://crates.io/crates/pipeliner
[`plmap`]: https://crates.io/crates/plmap
[`ordq`]: https://crates.io/crates/ordq
[`ordered-channel`]: https://crates.io/crates/ordered-channel
[`rayon-par-bridge`]: https://crates.io/crates/rayon-par-bridge
[`rayon`]: https://crates.io/crates/rayon

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
