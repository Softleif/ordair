# Changelog

## Unreleased

- `with_iter` runs the map and lends a closure an `Iterator` over the results,
  for `zip`, `take_while`, a `for` loop with `break` or an API that takes
  `impl Iterator`. The iterator cannot outlive the closure, so borrowing works
  as before and the call still never hangs, even if the iterator is
  `mem::forget`ten. A worker panic or an `init` error ends it early and comes
  back in the same `Result<Result<T, E>, Panic>`; dropping it early stops the
  workers. `try_for_each` is now a thin wrapper over it.
- The map no longer occupies its pool while the consumer catches up. Before, a
  worker blocked its thread whenever the window was full, so a `consume` that
  used the same pool hung: `install`, `join`, a parallel iterator, a library
  using rayon inside, or another map. With the default, the global pool, that
  was easy to run into. Now such a worker gives its thread back, and the
  consumer spawns a replacement for each result it takes. Using the pool from
  `work`, and chaining maps on one pool, work as well.
- Calling it from the only thread of a pool works now, rather than panicking:
  a consumer on a thread of the pool runs the pool's jobs while it waits.
- The window is now exact: no worker takes an item beyond it while it waits
  for room.
- `init`'s states are dropped on their threads once the call is done, by a
  `broadcast` on the pool, rather than as each worker stops.

## 0.3.0

Breaking: the outer error is now `Panic` instead of `WorkerPanic`, and it is
also what a panic in `consume` comes back as, rather than unwinding out of
`try_for_each`. So the code after the call runs whatever panicked, e.g. to
close what `consume` wrote to. `.or_unwind()` still resumes the panic.

- The error for a result that went missing although no worker failed now says
  that this is a bug in ordair, rather than reading like an ordinary worker
  panic.
- Documented that when both `init` and `consume` fail, `init`'s error is
  dropped.

## 0.2.0

Breaking: `try_for_each` returns `Result<Result<(), E>, WorkerPanic>` instead
of `Result<(), Error<E>>`. `Error` and `WorkerError` are gone.

- The inner error is your own, from `init` or `consume`, untouched, so an
  `eyre::Report` keeps its backtrace and span trace. Use `??`, or
  `.or_unwind()?` to resume a worker panic on the calling thread.
- `WorkerError::Panicked` is now the outer `WorkerPanic`, which is
  `Send + Sync` so that `?` converts it into an `anyhow` or `eyre` error.
- `Error::Both` is gone: when `init` and `consume` both fail, `consume`'s error
  is returned and `init`'s dropped, as a sequential loop would return. A worker
  panic wins over either.
- Depends on `rayon-core` instead of `rayon`.

### Migrating from 0.1

```rust
// 0.1
match ordair::in_order(items).map(work).try_for_each(consume) {
    Err(Error::Worker(WorkerError::Init(e))) | Err(Error::Consumer(e)) => …,
    Err(Error::Worker(WorkerError::Panicked(payload))) => …,
    Err(Error::Both { worker, consumer }) => …,
    Ok(()) => …,
}

// 0.2
ordair::in_order(items).map(work).try_for_each(consume)??;
```

## 0.1.0

First release.
