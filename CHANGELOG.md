# Changelog

## 0.3.1

- `Panic::origin()` says whether a worker or `consume` panicked, or whether a
  result went missing, which is a bug in ordair and no panic at all. Before,
  that was only in the `Display` text, next to the panic message, so a caller
  that already showed the message (e.g. from its panic hook) could not say
  what happened without repeating it.

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
