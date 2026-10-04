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
//! [`with_iter`](InOrder::with_iter) runs it too, but lends a closure an
//! [`Iterator`] over the results instead, for `zip`, `take_while`, a `for`
//! loop with `break` or an API that takes `impl Iterator`. The iterator cannot
//! leave the closure, which keeps the borrowing, the errors and the promise
//! below that it never hangs.
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
//! queued for the consumer *when the item is taken*. The queue is therefore in
//! input order by construction and needs no indices or reordering. `gzp` keeps
//! compressed blocks in order the same way.
//!
//! A worker takes a place in the window before it takes an item. When there is
//! none, it gives its thread back to the pool instead of waiting, and the
//! consumer spawns a replacement for each result it takes off the queue. So
//! the workers never hold a thread the consumer may need: `consume` and the
//! work can use the same pool, with `install`, `join`, a parallel iterator or
//! another map. A consumer on a thread of the pool runs the pool's jobs while
//! it waits for a result, as rayon's `join` does.

use rayon_core::ThreadPool;
use std::{
    any::Any,
    fmt, iter,
    panic::{self, AssertUnwindSafe},
    sync::{
        Mutex, MutexGuard, PoisonError, TryLockError,
        atomic::{AtomicUsize, Ordering},
        mpsc::{
            Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, channel, sync_channel,
        },
    },
    time::Duration,
};

mod states;
#[cfg(test)]
#[allow(clippy::arithmetic_side_effects, reason = "tests")]
mod tests;

use states::States;

/// Starts a parallel map over `items` whose results are consumed in their
/// order.
///
/// `items` can be anything that iterates, borrowed data included; it is
/// iterated from the worker threads, one item at a time. Nothing runs until
/// [`try_for_each`](InOrder::try_for_each) or
/// [`with_iter`](InOrder::with_iter).
pub fn in_order<I: IntoIterator>(items: I) -> InOrder<'static, I> {
    InOrder { items, pool: None, window: None, init: (), work: () }
}

/// A parallel map built by [`in_order`], run by
/// [`try_for_each`](Self::try_for_each) or [`with_iter`](Self::with_iter).
///
/// [`map`](Self::map) or [`map_init`](Self::map_init) sets the work;
/// [`pool`](Self::pool) and [`window`](Self::window) are optional and can be
/// set before or after it.
#[must_use = "nothing runs until `try_for_each` or `with_iter`"]
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
    /// While there is room in the window, every thread of the pool works on
    /// this map, so other work on the pool waits for the item a thread is on.
    /// Once the window is full, the workers give their threads back, so the
    /// consumer, the work and anything else can use the pool too.
    pub fn pool(self, pool: &ThreadPool) -> InOrder<'_, I, Init, Work> {
        let Self { items, pool: _, window, init, work } = self;
        InOrder { items, pool: Some(pool), window, init, work }
    }

    /// Bounds how far the workers run ahead of the consumer: besides the
    /// result the consumer is handling, at most `window` items have been
    /// started, running or finished and waiting.
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
    /// Calling this from one of the pool's own threads works, even from the
    /// only one: while that thread waits for a result, it runs the pool's
    /// jobs, the workers included.
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
        self.with_iter(|mut results| results.try_for_each(&mut consume))
    }

    /// Runs the map and lends `f` an iterator over the results, on the
    /// calling thread, in the order of the items; returns what `f` returns
    /// once the items in flight are done.
    ///
    /// The iterator is an ordinary [`Iterator`], so `zip`, `take_while`, a
    /// `for` loop with `break` or an API that takes `impl Iterator` all work,
    /// but it cannot outlive `f`. That is what lets the items, `work` and
    /// `init`'s state borrow, and what guarantees the workers stop whatever
    /// `f` does with the iterator.
    ///
    /// ```
    /// use std::io::Write;
    ///
    /// let words = ["alpha", "beta", "gamma", "delta"];
    /// let mut out = Vec::new();
    /// ordair::in_order(&words)
    ///     .map(|word| word.to_uppercase())
    ///     .with_iter(|lines| {
    ///         for line in lines.take(2) {
    ///             writeln!(out, "{line}")?;
    ///         }
    ///         Ok::<_, std::io::Error>(())
    ///     })??;
    /// assert_eq!(String::from_utf8(out)?, "ALPHA\nBETA\n");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// Dropping the iterator before it ends, or returning without finishing
    /// it, stops the workers from taking new items, like an error from
    /// `consume` in [`try_for_each`](Self::try_for_each) does. They finish the
    /// items in flight, and their results are dropped.
    ///
    /// # Errors
    ///
    /// A worker panic or an `init` error ends the iterator early, at the
    /// first item that has no result; the iterator cannot tell that apart from
    /// the end of the items, but the result can. As in
    /// [`try_for_each`](Self::try_for_each):
    ///
    /// - A panic in a worker or in `f` is the outer error, and wins over
    ///   everything else, even when `f` stopped before the item that panicked.
    /// - An error from `f` wins over an error from `init`.
    /// - An error from `init` is returned when the iterator ended at the item
    ///   it left without a result, even when `f` returned `Ok`. Had `f`
    ///   stopped before that item, it is dropped, as a sequential loop would
    ///   never have reached it.
    pub fn with_iter<S, R, E, T>(
        self,
        f: impl for<'a> FnOnce(Iter<'a, R>) -> Result<T, E>,
    ) -> Result<Result<T, E>, Panic>
    where
        I: IntoIterator<IntoIter: Send, Item: Send>,
        Init: Fn() -> Result<S, E> + Sync,
        Work: Fn(&mut S, I::Item) -> R + Sync,
        R: Send,
        E: Send,
    {
        let Self { items, pool, window, init, work } = self;
        let threads =
            pool.map_or_else(rayon_core::current_num_threads, ThreadPool::current_num_threads);
        let window = window.unwrap_or_else(|| threads.saturating_mul(4));
        let (pending, results) = channel();
        let map = Map {
            queue: Queue::new(items.into_iter(), pending),
            gate: Gate::new(window),
            states: States::new(threads),
            init,
            work,
            // The first of each: when several workers fail, they mostly share
            // a cause.
            init_failure: Mutex::new(None),
            worker_panic: Mutex::new(None),
        };

        let (consumed, gap) = in_place_scope(pool, |scope| {
            let spawn_worker = || scope.spawn(|_| map.run_worker());
            for _ in 0..threads {
                spawn_worker();
            }
            // A worker that found the window full gave its thread back; one
            // takes its place for each result the consumer takes.
            let release = || {
                if map.gate.release() {
                    spawn_worker();
                }
            };

            // `f` only borrows this, so the receiver is dropped here whatever
            // `f` did with the iterator, `mem::forget` included.
            let mut consumer = Consumer { results: Some(results), gap: false };
            let consumed = panic::catch_unwind(AssertUnwindSafe(|| {
                f(Iter { consumer: &mut consumer, release: &release })
            }));
            // Workers queueing a result find a disconnected channel and stop.
            consumer.results = None;
            (consumed, consumer.gap)
        });
        // Each state on its own thread, as it need not be `Send`.
        map.drop_states(pool);
        let Map { init_failure, worker_panic, .. } = map;

        let consumed = consumed.map_err(|payload| Panic::new(payload, true))?;
        if let Some(payload) = worker_panic.into_inner().unwrap_or_else(PoisonError::into_inner) {
            return Err(Panic::new(payload, false));
        }
        match (consumed, gap) {
            (Err(consumer), _) => Ok(Err(consumer)),
            (Ok(value), false) => Ok(Ok(value)),
            (Ok(_), true) => {
                match init_failure.into_inner().unwrap_or_else(PoisonError::into_inner) {
                    Some(init) => Ok(Err(init)),
                    // Only a failed worker leaves a result unsent, and it
                    // recorded why.
                    None => Err(Panic::new(Box::new(LOST_RESULT), false)),
                }
            }
        }
    }
}

/// The results of a map, in the order of its items, lent to the closure of
/// [`with_iter`](InOrder::with_iter).
///
/// It ends when the items do, or early at the first item that has no result
/// because a worker panicked or its `init` failed; `with_iter`'s result says
/// which. Dropping it stops the workers from taking new items.
///
/// It cannot leave the closure:
///
/// ```compile_fail
/// let words = ["alpha", "beta"];
/// let escaped = ordair::in_order(&words)
///     .map(|word| word.len())
///     .with_iter(|lengths| Ok::<_, ()>(lengths));
/// ```
pub struct Iter<'a, R> {
    consumer: &'a mut Consumer<R>,
    /// Called for each result taken off the queue, which frees its place in
    /// the window.
    release: &'a (dyn Fn() + Sync + 'a),
}

/// What the consumer's side of [`with_iter`](InOrder::with_iter) owns, so that
/// it is cleaned up even if the [`Iter`] borrowing it is forgotten.
struct Consumer<R> {
    /// The receivers for the results, in order; `None` once the iterator
    /// ended or was dropped.
    results: Option<Receiver<Receiver<R>>>,
    /// Whether the iterator ended at an item that has no result.
    gap: bool,
}

impl<R> Iterator for Iter<'_, R> {
    type Item = R;

    fn next(&mut self) -> Option<R> {
        let next = wait(self.consumer.results.as_ref()?);
        if next.is_some() {
            (self.release)();
        }
        match next.map(|result| wait(&result)) {
            Some(Some(result)) => Some(result),
            ended => {
                // A result that never came.
                self.consumer.gap = ended.is_some();
                self.consumer.results = None;
                None
            }
        }
    }
}

impl<R> iter::FusedIterator for Iter<'_, R> {}

/// Receives from `channel`, or `None` once it is disconnected.
///
/// On a thread of a rayon pool, it runs the pool's other jobs while it
/// waits, as rayon's own `join` does: the workers of this very map, if the
/// consumer is one of the threads they need, or work it is holding up, such
/// as a `broadcast` from the work. Workers never wait for the consumer, so
/// none of them can hold it up for long. Rayon cannot wake a thread for a new
/// job, so it looks again after at most a millisecond.
fn wait<T>(channel: &Receiver<T>) -> Option<T> {
    if rayon_core::current_thread_index().is_none() {
        return channel.recv().ok();
    }
    let mut pause = Duration::from_micros(1);
    loop {
        match channel.try_recv() {
            Ok(value) => return Some(value),
            Err(TryRecvError::Disconnected) => return None,
            Err(TryRecvError::Empty) => {}
        }
        if rayon_core::yield_now() == Some(rayon_core::Yield::Executed) {
            pause = Duration::from_micros(1);
            continue;
        }
        match channel.recv_timeout(pause) {
            Ok(value) => return Some(value),
            Err(RecvTimeoutError::Disconnected) => return None,
            Err(RecvTimeoutError::Timeout) => {
                pause = pause.saturating_mul(2).min(Duration::from_millis(1))
            }
        }
    }
}

impl<R> Drop for Iter<'_, R> {
    fn drop(&mut self) {
        self.consumer.results = None;
    }
}

impl<R> fmt::Debug for Iter<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iter").field("ended", &self.consumer.results.is_none()).finish()
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

/// What the workers of one call share.
struct Map<I: Iterator, S, R, E, Init, Work> {
    queue: Queue<I, R>,
    gate: Gate,
    states: States<S>,
    init: Init,
    work: Work,
    init_failure: Mutex<Option<E>>,
    worker_panic: Mutex<Option<Box<dyn Any + Send>>>,
}

/// Why a worker stopped without failing.
enum Stopped {
    /// It gave its thread back, to be replaced when there is room.
    Parked,
    /// There is nothing more to take.
    Done,
}

impl<I, S, R, E, Init, Work> Map<I, S, R, E, Init, Work>
where
    I: Iterator,
    Init: Fn() -> Result<S, E>,
    Work: Fn(&mut S, I::Item) -> R,
{
    /// A worker: takes items while there is room in the window and returns
    /// once there is none, or nothing left to take. It never waits for the
    /// consumer, so the consumer can use the pool too.
    fn run_worker(&self) {
        // `None` on a thread that is already working on this map further up
        // its stack, having stolen this job while waiting in `work`: that
        // thread's state is in use. The worker below still has an item in
        // flight, whose result will spawn a replacement.
        let Some(mut state) = self.states.claim() else {
            self.gate.park();
            return;
        };
        let worked =
            panic::catch_unwind(AssertUnwindSafe(|| self.take_items(state.get(), &self.states)));
        drop(state);
        match worked {
            Ok(Ok(Stopped::Parked | Stopped::Done)) => {}
            // If every worker failed and none closed the queue, the consumer
            // would wait forever.
            Ok(Err(error)) => {
                self.queue.close();
                keep_first(&self.init_failure, error);
            }
            Err(payload) => {
                self.queue.close();
                keep_first(&self.worker_panic, payload);
            }
        }
    }

    fn take_items(&self, state: &mut Option<S>, states: &States<S>) -> Result<Stopped, E> {
        loop {
            if !self.gate.enter() {
                return Ok(Stopped::Parked);
            }
            let (item, place) = match self.queue.take() {
                Take::Item(item) => item,
                Take::Busy => {
                    self.gate.leave_and_park();
                    return Ok(Stopped::Parked);
                }
                Take::Done => return Ok(Stopped::Done),
            };
            let state = match state {
                Some(state) => state,
                // If this fails, the item is dropped unanswered, which is
                // where the consumer stops.
                None => states.built(state.insert((self.init)()?)),
            };
            if place.send((self.work)(state, item)).is_err() {
                // The consumer stopped, so this result is not needed.
                self.queue.close();
                return Ok(Stopped::Done);
            }
        }
    }

    /// Drops the state of every thread that built one, on that thread.
    fn drop_states(&self, pool: Option<&ThreadPool>) {
        if !self.states.any_built() {
            return;
        }
        let (states, worker_panic) = (&self.states, &self.worker_panic);
        let drop_mine = |_: rayon_core::BroadcastContext<'_>| {
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| states.drop_mine())) {
                keep_first(worker_panic, payload);
            }
        };
        match pool {
            Some(pool) => pool.broadcast(drop_mine),
            None => rayon_core::broadcast(drop_mine),
        };
    }
}

/// The payload of the [`Panic`] for a result that went missing although
/// no worker failed.
const LOST_RESULT: &str =
    "A result went missing although no worker failed, which is a bug in ordair";

fn keep_first<T>(first: &Mutex<Option<T>>, failure: T) {
    first.lock().unwrap_or_else(PoisonError::into_inner).get_or_insert(failure);
}

/// A worker or the consumer panicked, the outer error of
/// [`try_for_each`](InOrder::try_for_each) and [`with_iter`](InOrder::with_iter);
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
/// [`try_for_each`](InOrder::try_for_each) and [`with_iter`](InOrder::with_iter).
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

/// The room left in the window, and how many workers gave their thread
/// back for want of it.
struct Gate(Mutex<Room>);

struct Room {
    free: usize,
    parked: usize,
}

impl Gate {
    fn new(window: usize) -> Self {
        Self(Mutex::new(Room { free: window, parked: 0 }))
    }

    fn lock(&self) -> MutexGuard<'_, Room> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a place in the window for an item, or, when there is none,
    /// parks the worker asking.
    fn enter(&self) -> bool {
        let mut room = self.lock();
        if let Some(free) = room.free.checked_sub(1) {
            room.free = free;
            true
        } else {
            room.parked = room.parked.saturating_add(1);
            false
        }
    }

    /// Parks a worker that holds no place in the window.
    fn park(&self) {
        let mut room = self.lock();
        room.parked = room.parked.saturating_add(1);
    }

    /// Gives back the place a worker took, and parks it.
    fn leave_and_park(&self) {
        let mut room = self.lock();
        room.free = room.free.saturating_add(1);
        room.parked = room.parked.saturating_add(1);
    }

    /// Frees the place of a result the consumer took. True when a parked
    /// worker should be spawned to take it.
    fn release(&self) -> bool {
        let mut room = self.lock();
        room.free = room.free.saturating_add(1);
        let Some(parked) = room.parked.checked_sub(1) else { return false };
        room.parked = parked;
        true
    }
}

/// Marks the thread holding the [`Queue`]'s items until dropped, unwinding
/// included.
struct Holding<'a>(&'a AtomicUsize);

impl<'a> Holding<'a> {
    fn new(holder: &'a AtomicUsize, me: usize) -> Self {
        holder.store(me, Ordering::SeqCst);
        Self(holder)
    }
}

impl Drop for Holding<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::SeqCst);
    }
}

/// What a worker got from the [`Queue`].
enum Take<T> {
    Item(T),
    /// A worker further up this thread's stack is taking an item, so waiting
    /// for it would never end.
    Busy,
    /// There is nothing more to take.
    Done,
}

/// The items not taken yet, and where the receivers for their results go.
/// `None` once closed.
struct Queue<I: Iterator, R> {
    items: Mutex<Option<(I, Sender<Receiver<R>>)>>,
    /// One more than the index of the thread holding `items`, else zero.
    holder: AtomicUsize,
}

impl<I: Iterator, R> Queue<I, R> {
    fn new(items: I, pending: Sender<Receiver<R>>) -> Self {
        Self { items: Mutex::new(Some((items, pending))), holder: AtomicUsize::new(0) }
    }

    /// Takes the next item and queues the receiver for its result in the same
    /// critical section, which is what keeps the queue in input order.
    ///
    /// Once there is nothing more to take, for whatever reason, the queue is
    /// closed so that the consumer sees the end of its results.
    fn take(&self) -> Take<(I::Item, SyncSender<R>)> {
        // One more than this thread's index; a worker always runs on one.
        let me = rayon_core::current_thread_index().map_or(0, |index| index.saturating_add(1));
        let mut queue = match self.items.try_lock() {
            Ok(queue) => queue,
            Err(TryLockError::Poisoned(poisoned)) => Self::closed(poisoned.into_inner()),
            // Its holder, if it is this thread, is further up the stack, in
            // the items' `next`, and has stolen this worker while waiting
            // there. The holder only ever sets it to its own index, and resets
            // it before letting go, so this cannot see this thread by mistake.
            Err(TryLockError::WouldBlock)
                if me != 0 && self.holder.load(Ordering::SeqCst) == me =>
            {
                return Take::Busy;
            }
            Err(TryLockError::WouldBlock) => self.lock(),
        };
        let holding = Holding::new(&self.holder, me);
        let next = queue.as_mut().and_then(|(items, pending)| {
            let item = items.next()?;
            let (place, result) = sync_channel(1);
            pending.send(result).ok()?;
            Some((item, place))
        });
        drop(holding);
        match next {
            Some(next) => Take::Item(next),
            None => {
                *queue = None;
                Take::Done
            }
        }
    }

    fn close(&self) {
        *self.lock() = None;
    }

    fn lock(&self) -> MutexGuard<'_, Option<(I, Sender<Receiver<R>>)>> {
        self.items.lock().unwrap_or_else(|poisoned| Self::closed(poisoned.into_inner()))
    }

    /// A panic while holding the lock leaves the iterator in an unknown state,
    /// so a poisoned queue is a closed one.
    fn closed(
        mut queue: MutexGuard<'_, Option<(I, Sender<Receiver<R>>)>>,
    ) -> MutexGuard<'_, Option<(I, Sender<Receiver<R>>)>> {
        *queue = None;
        queue
    }
}
