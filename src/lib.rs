//! A parallel map whose results are consumed in input order, with a hard
//! bound on how many results can wait for the consumer.
//!
//! [`in_order`] takes the items, [`map`](InOrder::map) or
//! [`map_init`](InOrder::map_init) says what to do with each one on the
//! threads of a rayon pool, and [`try_for_each`](InOrder::try_for_each) runs
//! it, handing each result to a closure on the calling thread, in the order
//! of the items:
//!
//! ```
//! use std::io::Write;
//!
//! let mut out = Vec::new();
//! ordair::in_order(1..=1000_u64)
//!     .map(|n| (1..=n).map(|k| k * k).sum::<u64>())
//!     // `??`: the outer `?` is a panic, the inner one is your own error.
//!     .try_for_each(|sum| writeln!(out, "{sum}"))??;
//! assert!(out.starts_with(b"1\n5\n14\n30\n"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Per-worker state, the pool and the window
//!
//! [`map_init`](InOrder::map_init) builds some state once per worker thread,
//! on that thread, and hands it to every call on that thread: a reader, a
//! connection, a scratch buffer. Building it may fail, and it need not be
//! `Send`.
//!
//! By default the work runs on the current rayon pool, the global one unless
//! called from inside another, with a window of four results per thread.
//! [`pool`](InOrder::pool) and [`window`](InOrder::window) change that:
//!
//! ```
//! use std::io::Write;
//!
//! let pool = rayon_core::ThreadPoolBuilder::new()
//!     .num_threads(4)
//!     .thread_name(|i| format!("reverse-{i}"))
//!     .build()?;
//! let words = ["alpha", "beta", "gamma", "delta"];
//! let mut out = Vec::new();
//! ordair::in_order(&words)
//!     .pool(&pool)
//!     .window(8)
//!     .map_init(
//!         || Ok(String::with_capacity(64)),
//!         |buffer, word| {
//!             buffer.clear();
//!             buffer.extend(word.chars().rev());
//!             buffer.to_uppercase()
//!         },
//!     )
//!     .try_for_each(|line| writeln!(out, "{line}"))??;
//! assert_eq!(String::from_utf8(out)?, "AHPLA\nATEB\nAMMAG\nATLED\n");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Errors
//!
//! A panic in a worker or in the consumer, or an error from `init` or from the
//! consumer, stops the workers from taking new items and is returned once the
//! items in flight are done. The result is two: the outer error is a
//! [`Panic`], which [`or_unwind`](OrUnwind::or_unwind) resumes, and the inner
//! one is your own error, untouched, so `??` or `.or_unwind()?` keeps what it
//! captured, such as an `eyre::Report`'s backtrace. Either way, the code after
//! the call runs, e.g. to close what the consumer wrote to. It never hangs.
//!
//! ```
//! use ordair::OrUnwind;
//!
//! let mut consumed = 0;
//! let result = ordair::in_order(0..100)
//!     .map_init(|| Err::<(), _>("no licence left"), |(), n| n)
//!     .try_for_each(|_| {
//!         consumed += 1;
//!         Ok(())
//!     })
//!     .or_unwind();
//! // The item a worker took before its `init` failed has no result, so
//! // the consumer stops there.
//! assert_eq!(result, Err("no licence left"));
//! assert_eq!(consumed, 0);
//! ```
//!
//! `work` has no error of its own: when it can fail, it returns a `Result`
//! and `consume` passes the error on with `?`, so it comes back as yours,
//! from the item it belongs to. `init` and `consume` share one error type,
//! which `anyhow` and `eyre` make easy; with concrete types, one of them may
//! need a `map_err`.
//!
//! ```
//! use ordair::OrUnwind;
//! use std::num::ParseIntError;
//!
//! let mut total = 0;
//! let result = ordair::in_order(["1", "2", "three", "4"])
//!     .map(|word| word.parse::<u32>())
//!     .try_for_each(|n| {
//!         total += n?;
//!         Ok::<_, ParseIntError>(())
//!     })
//!     .or_unwind();
//! assert_eq!(result.unwrap_err().to_string(), "invalid digit found in string");
//! assert_eq!(total, 3);
//! ```
//!
//! # How it works
//!
//! Each item gets a one-shot channel for its result, and the receiving end is
//! queued for the consumer *when the item is taken*, in a bounded channel. The
//! queue is therefore in input order by construction, needs no indices or
//! reordering, and a worker that would start more than `window` items ahead of
//! the consumer blocks until the consumer catches up. `gzp` keeps compressed
//! blocks in order the same way.

use rayon_core::ThreadPool;
use std::{
    any::Any,
    fmt, iter,
    panic::{self, AssertUnwindSafe},
    sync::{
        Mutex, MutexGuard, PoisonError,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread,
};

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects, reason = "tests")]
mod tests;

/// Starts a parallel map over `items` whose results are consumed in their
/// order.
///
/// `items` can be anything that iterates, borrowed data included; it is
/// iterated from the worker threads, one item at a time. Nothing runs until
/// [`try_for_each`](InOrder::try_for_each).
pub fn in_order<I: IntoIterator>(items: I) -> InOrder<'static, I> {
    InOrder { items, pool: None, window: None, init: (), work: () }
}

/// A parallel map built by [`in_order`], run by
/// [`try_for_each`](Self::try_for_each).
///
/// [`map`](Self::map) or [`map_init`](Self::map_init) sets the work;
/// [`pool`](Self::pool) and [`window`](Self::window) are optional and can be
/// set before or after it.
#[must_use = "nothing runs until `try_for_each`"]
pub struct InOrder<'p, I, Init = (), Work = ()> {
    items: I,
    pool: Option<&'p ThreadPool>,
    window: Option<usize>,
    init: Init,
    work: Work,
}

impl<I, Init, Work> InOrder<'_, I, Init, Work> {
    /// Runs the work on the threads of `pool` instead of the current rayon
    /// pool, for its size, thread names or handlers.
    ///
    /// Every thread of the pool works on this map until the items run out,
    /// so other work on the pool waits, and a parallel iterator inside the
    /// work only gets the threads that have run out of items.
    pub fn pool(self, pool: &ThreadPool) -> InOrder<'_, I, Init, Work> {
        let Self { items, pool: _, window, init, work } = self;
        InOrder { items, pool: Some(pool), window, init, work }
    }

    /// Bounds how far the workers run ahead of the consumer: besides the
    /// result the consumer is handling, at most `window` items have been
    /// started, running or finished and waiting. One more may have been
    /// taken from the items by a worker waiting for room.
    ///
    /// A result that is finished but waits behind a slow item still counts,
    /// so memory stays bounded however uneven the items are. To keep every
    /// thread busy behind one slow item, it should be a few times the number
    /// of threads; the default is four times.
    ///
    /// # Panics
    ///
    /// When `window` is zero.
    pub fn window(mut self, window: usize) -> Self {
        assert!(window > 0, "the window of `ordair::in_order` must not be zero");
        self.window = Some(window);
        self
    }
}

impl<'p, I: IntoIterator> InOrder<'p, I> {
    /// Calls `work` with each item, on the thread that took it.
    #[allow(clippy::type_complexity, reason = "the closures are unnameable anyway")]
    pub fn map<R, E>(
        self,
        work: impl Fn(I::Item) -> R + Sync,
    ) -> InOrder<'p, I, impl Fn() -> Result<(), E> + Sync, impl Fn(&mut (), I::Item) -> R + Sync>
    {
        self.map_init(|| Ok(()), move |(), item| work(item))
    }

    /// Calls `work` with each item and the state of the worker thread that
    /// took it.
    ///
    /// `init` builds that state when the thread takes its first item, on that
    /// thread, so it need not be `Send` and a thread that gets no item builds
    /// none. Unlike rayon's `map_init`, which builds state for every piece it
    /// splits the work into, this is once per thread. When `init` fails, that
    /// item has no result and the consumer stops before it; see
    /// [`try_for_each`](Self::try_for_each).
    ///
    /// ```
    /// use std::{cell::RefCell, rc::Rc};
    ///
    /// let mut total = 0;
    /// ordair::in_order(0..100_u32)
    ///     // Not `Send`, and not a problem.
    ///     .map_init(|| Ok(Rc::new(RefCell::new(Vec::new()))), |seen, n| {
    ///         seen.borrow_mut().push(n);
    ///         n * 2
    ///     })
    ///     .try_for_each(|doubled| {
    ///         total += doubled;
    ///         Ok::<_, std::io::Error>(())
    ///     })??;
    /// assert_eq!(total, 9900);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn map_init<S, R, E, Init, Work>(self, init: Init, work: Work) -> InOrder<'p, I, Init, Work>
    where
        Init: Fn() -> Result<S, E> + Sync,
        Work: Fn(&mut S, I::Item) -> R + Sync,
    {
        let Self { items, pool, window, init: (), work: () } = self;
        InOrder { items, pool, window, init, work }
    }
}

impl<I, Init, Work> InOrder<'_, I, Init, Work> {
    /// Runs the map, handing each result to `consume` on the calling thread,
    /// in the order of the items, and returns once the items run out or
    /// something failed.
    ///
    /// Calling this from one of the pool's own threads works, but that thread
    /// consumes instead of working.
    ///
    /// # Errors
    ///
    /// When a worker or `consume` panics, or `init` or `consume` returns an
    /// error: that stops the workers from taking new items, and comes back
    /// once the items in flight are done. Results stop at the first item that
    /// has none.
    ///
    /// A panic is the outer error, which [`or_unwind`](OrUnwind::or_unwind)
    /// resumes. The inner one is your own, untouched, so a `?` keeps whatever
    /// it captured. A panic wins over an error; `consume`'s wins over a
    /// worker's or `init`'s, as it always comes from an earlier item. The
    /// losing one is dropped, as a sequential loop would never have reached
    /// its item; to see it anyway, log it in `init`.
    ///
    /// # Panics
    ///
    /// When called from the only thread of the pool, which would leave no
    /// thread to work.
    pub fn try_for_each<S, R, E>(
        self,
        mut consume: impl FnMut(R) -> Result<(), E>,
    ) -> Result<Result<(), E>, Panic>
    where
        I: IntoIterator<IntoIter: Send, Item: Send>,
        Init: Fn() -> Result<S, E> + Sync,
        Work: Fn(&mut S, I::Item) -> R + Sync,
        R: Send,
        E: Send,
    {
        let Self { items, pool, window, init, work } = self;
        let (on_a_pool_thread, threads) = match pool {
            Some(pool) => (pool.current_thread_index().is_some(), pool.current_num_threads()),
            None => {
                (rayon_core::current_thread_index().is_some(), rayon_core::current_num_threads())
            }
        };
        assert!(!on_a_pool_thread || threads > 1, "ordair called from the only thread of its pool");
        let window = window.unwrap_or_else(|| threads.saturating_mul(4));
        let (pending, results) = sync_channel(window);
        let queue = Queue::new(items.into_iter(), pending);
        // The first of each: when several workers fail, they mostly share a
        // cause.
        let init_failure = Mutex::new(None);
        let worker_panic = Mutex::new(None);

        let consumed = in_place_scope(pool, |scope| {
            scope.spawn_broadcast(|_, _| {
                let worked = run_worker(&queue, &init, &work);
                // However the worker stopped: if every worker panicked in
                // `init` and none closed the queue, the consumer would wait
                // forever.
                queue.close();
                match worked {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => keep_first(&init_failure, error),
                    Err(payload) => keep_first(&worker_panic, payload),
                }
            });

            // `Err(None)` is a result that never came.
            let consumed = panic::catch_unwind(AssertUnwindSafe(|| {
                results.iter().try_for_each(|result: Receiver<R>| match result.recv() {
                    Ok(result) => consume(result).map_err(Some),
                    Err(_) => Err(None),
                })
            }));
            // Workers blocked on queueing a result wake up to a disconnected
            // channel and stop.
            drop(results);
            consumed
        });

        let consumed = consumed.map_err(|payload| Panic::new(payload, true))?;
        if let Some(payload) = worker_panic.into_inner().unwrap_or_else(PoisonError::into_inner) {
            return Err(Panic::new(payload, false));
        }
        match (consumed, init_failure.into_inner().unwrap_or_else(PoisonError::into_inner)) {
            (Ok(()), None) => Ok(Ok(())),
            (Err(Some(consumer)), _) => Ok(Err(consumer)),
            (Ok(()) | Err(None), Some(init)) => Ok(Err(init)),
            // Only a failed worker leaves a result unsent, and it recorded why.
            (Err(None), None) => Err(Panic::new(Box::new(LOST_RESULT), false)),
        }
    }
}

/// `pool`'s scope, or the current pool's.
fn in_place_scope<'scope, R>(
    pool: Option<&ThreadPool>,
    op: impl FnOnce(&rayon_core::Scope<'scope>) -> R,
) -> R {
    match pool {
        Some(pool) => pool.in_place_scope(op),
        None => rayon_core::in_place_scope(op),
    }
}

/// Takes items until there are none left or the consumer is gone. The outer
/// error is a panic.
fn run_worker<I: Iterator, S, R, E>(
    queue: &Queue<I, R>,
    init: &impl Fn() -> Result<S, E>,
    work: &impl Fn(&mut S, I::Item) -> R,
) -> thread::Result<Result<(), E>> {
    panic::catch_unwind(AssertUnwindSafe(|| {
        let Some(first) = queue.take() else { return Ok(()) };
        // If this fails, the first item is dropped unanswered, which is where
        // the consumer stops.
        let mut state = init()?;
        for (item, place) in iter::once(first).chain(iter::from_fn(|| queue.take())) {
            if place.send(work(&mut state, item)).is_err() {
                // The consumer stopped, so this result is not needed.
                break;
            }
        }
        Ok(())
    }))
}

/// The payload of the [`Panic`] for a result that went missing although
/// no worker failed.
const LOST_RESULT: &str =
    "A result went missing although no worker failed, which is a bug in ordair";

fn keep_first<T>(first: &Mutex<Option<T>>, failure: T) {
    first.lock().unwrap_or_else(PoisonError::into_inner).get_or_insert(failure);
}

/// A worker or the consumer panicked, the outer error of
/// [`try_for_each`](InOrder::try_for_each);
/// [`or_unwind`](OrUnwind::or_unwind) resumes it.
///
/// It holds the payload `std::panic::catch_unwind` gives, behind a mutex
/// only so that it is `Sync`, which `anyhow` and `eyre` need. A result that
/// goes missing although no worker failed, a bug in ordair, comes back as one
/// too, with a message saying so.
pub struct Panic {
    payload: Mutex<Box<dyn Any + Send>>,
    in_consumer: bool,
}

impl Panic {
    fn new(payload: Box<dyn Any + Send>, in_consumer: bool) -> Self {
        Self { payload: Mutex::new(payload), in_consumer }
    }

    /// The payload, as `std::panic::catch_unwind` gives it.
    pub fn into_payload(self) -> Box<dyn Any + Send> {
        self.payload.into_inner().unwrap_or_else(PoisonError::into_inner)
    }

    /// Continues the panic on this thread. The panic hook ran when it
    /// panicked and does not run again.
    pub fn resume(self) -> ! {
        panic::resume_unwind(self.into_payload())
    }
}

/// [`or_unwind`](Self::or_unwind), for the result of
/// [`try_for_each`](InOrder::try_for_each).
pub trait OrUnwind<T> {
    /// The value, or the panic resumed on this thread, as if the work had run
    /// there.
    fn or_unwind(self) -> T;
}

impl<T> OrUnwind<T> for Result<T, Panic> {
    fn or_unwind(self) -> T {
        self.unwrap_or_else(|panic| panic.resume())
    }
}

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = self.payload.lock().unwrap_or_else(PoisonError::into_inner);
        match message(&**payload) {
            LOST_RESULT => f.write_str(LOST_RESULT),
            message if self.in_consumer => write!(f, "The consumer panicked: {message}"),
            message => write!(f, "A worker panicked: {message}"),
        }
    }
}

impl fmt::Debug for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = self.payload.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("Panic")
            .field("in_consumer", &self.in_consumer)
            .field("message", &message(&**payload))
            .finish()
    }
}

impl std::error::Error for Panic {}

fn message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

/// The items not taken yet, and where the receivers for their results go.
/// `None` once closed.
struct Queue<I: Iterator, R>(Mutex<Option<(I, SyncSender<Receiver<R>>)>>);

impl<I: Iterator, R> Queue<I, R> {
    fn new(items: I, pending: SyncSender<Receiver<R>>) -> Self {
        Self(Mutex::new(Some((items, pending))))
    }

    /// Takes the next item and queues the receiver for its result in the same
    /// critical section, which is what keeps the queue in input order.
    /// Blocking here while the queue is full is the back-pressure.
    ///
    /// Once there is nothing more to take, for whatever reason, the queue is
    /// closed so that the consumer sees the end of its results.
    fn take(&self) -> Option<(I::Item, SyncSender<R>)> {
        let mut queue = self.lock();
        let next = queue.as_mut().and_then(|(items, pending)| {
            let item = items.next()?;
            let (place, result) = sync_channel(1);
            pending.send(result).ok()?;
            Some((item, place))
        });
        if next.is_none() {
            *queue = None;
        }
        next
    }

    fn close(&self) {
        *self.lock() = None;
    }

    /// A panic while holding the lock leaves the iterator in an unknown state,
    /// so a poisoned queue is a closed one.
    fn lock(&self) -> MutexGuard<'_, Option<(I, SyncSender<Receiver<R>>)>> {
        self.0.lock().unwrap_or_else(|poisoned| {
            let mut queue = poisoned.into_inner();
            *queue = None;
            queue
        })
    }
}
