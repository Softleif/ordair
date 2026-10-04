//! The map shares its pool: the consumer and the work may use it too,
//! whatever the pool's size, the window or where the call is made from.
//!
//! "It never hangs" is the contract under test; a hang is caught by
//! running every case on its own thread with a deadline.

use super::*;
use hegel::{Generator, TestCase, generators as gs};
use std::{
    cell::Cell,
    panic::{self, AssertUnwindSafe},
    sync::{Mutex, atomic::AtomicIsize, mpsc},
    thread::ThreadId,
};

/// Something done with the map's pool, from the consumer or the work.
#[derive(Debug, Clone, Copy)]
enum Use {
    Nothing,
    /// `install` a closure that does nothing.
    Install,
    /// Two halves of a `join`, each taking this long.
    Join(u64),
    /// Run a closure on every thread of the pool.
    Broadcast,
    /// Another map on the same pool, over `0..len`, consuming the first
    /// `take` results if given, else all.
    Nested {
        len: usize,
        window: usize,
        take: Option<usize>,
    },
}

/// Where the map is called from.
#[derive(Debug, Clone, Copy)]
enum Caller {
    /// A thread outside the pool, with `.pool(&pool)`.
    Outside,
    /// One of the pool's own threads, with the current pool.
    Inside,
}

#[derive(Debug, Clone)]
struct Scenario {
    threads: usize,
    window: usize,
    caller: Caller,
    /// For each item: how long the work takes, what it does with the pool,
    /// and what the consumer does with it.
    items: Vec<(u64, Use, Use)>,
}

fn below(n: usize) -> impl Generator<usize> {
    gs::integers::<usize>().max_value(n - 1)
}

#[hegel::composite]
fn pool_use(tc: &TestCase) -> Use {
    match tc.draw_silent(below(8)) {
        0 => Use::Install,
        1 => Use::Join(tc.draw_silent(gs::integers::<u64>().max_value(200))),
        2 => Use::Broadcast,
        3 => {
            let len = tc.draw_silent(below(12));
            let take = tc.draw_silent(gs::booleans()).then(|| tc.draw_silent(below(len + 1)));
            Use::Nested { len, window: tc.draw_silent(below(4)) + 1, take }
        }
        _ => Use::Nothing,
    }
}

#[hegel::composite]
fn latency(tc: &TestCase) -> u64 {
    if tc.draw_silent(gs::weighted_booleans(0.2)) {
        tc.draw_silent(gs::integers::<u64>().max_value(499))
    } else {
        0
    }
}

/// `consumer_uses` and `work_uses` say whether each side draws uses of
/// the pool, so that each property can fail on its own side alone.
fn scenario(work_uses: bool, consumer_uses: bool) -> impl Generator<Scenario> {
    hegel::compose!(|tc| {
        let threads = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(6));
        let window = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(8));
        let caller = if tc.draw_silent(gs::booleans()) { Caller::Inside } else { Caller::Outside };
        // Long enough for the window to fill.
        let len = tc.draw_silent(below(40));
        let items = (0..len)
            .map(|_| {
                let work = if work_uses { tc.draw_silent(pool_use()) } else { Use::Nothing };
                let consume = if consumer_uses { tc.draw_silent(pool_use()) } else { Use::Nothing };
                (tc.draw_silent(latency()), work, consume)
            })
            .collect();
        Scenario { threads, window, caller, items }
    })
}

/// Uses the pool as `what` says and returns a value to check: the sum of
/// the results consumed from a nested map, else zero.
fn use_pool(pool: &ThreadPool, caller: Caller, what: Use) -> usize {
    match what {
        Use::Nothing => 0,
        Use::Install => pool.install(|| 0),
        Use::Join(us) => {
            let sleep = || thread::sleep(Duration::from_micros(us));
            pool.install(|| rayon_core::join(sleep, sleep));
            0
        }
        Use::Broadcast => {
            pool.broadcast(|_| ());
            0
        }
        Use::Nested { len, window, take } => {
            let nested = in_order(0..len).window(window);
            let nested = match caller {
                Caller::Outside => nested.pool(pool),
                // The current pool, which is this one.
                Caller::Inside => nested,
            };
            nested
                .map(|i| i)
                .with_iter(|results| Ok::<_, String>(results.take(take.unwrap_or(len)).sum()))
                .or_unwind()
                .unwrap()
        }
    }
}

/// What a nested use returns, by the same rules.
fn expected(what: Use) -> usize {
    match what {
        Use::Nested { len, take, .. } => (0..take.unwrap_or(len).min(len)).sum(),
        _ => 0,
    }
}

/// A worker's state, which checks that it is only ever used on the thread
/// that built it, by one call at a time, and dropped there.
struct State<'a> {
    built_on: ThreadId,
    in_use: Cell<bool>,
    record: &'a Record,
}

impl Drop for State<'_> {
    fn drop(&mut self) {
        if self.built_on != thread::current().id() {
            self.record.broke("a state was dropped on another thread".to_owned());
        }
        self.record.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct Record {
    inits: AtomicUsize,
    live: AtomicIsize,
    broken: Mutex<Vec<String>>,
}

impl Record {
    fn broke(&self, promise: String) {
        self.broken.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(promise);
    }
}

struct Outcome {
    result: Result<Result<(), String>, Panic>,
    /// For each consumed item, its index and the values its uses returned.
    consumed: Vec<(usize, usize, usize)>,
    /// How many states were alive when the call returned.
    live_after: isize,
    record: Record,
}

fn run(scenario: &Scenario) -> Outcome {
    let Scenario { threads, window, caller, items } = scenario;
    let pool = pool(*threads);
    let record = Record::default();
    let mut consumed = Vec::new();
    let mut call = || {
        let map = in_order(items.iter().enumerate()).window(*window);
        let map = match caller {
            Caller::Outside => map.pool(&pool),
            Caller::Inside => map,
        };
        map.map_init(
            || {
                record.inits.fetch_add(1, Ordering::SeqCst);
                record.live.fetch_add(1, Ordering::SeqCst);
                Ok(State {
                    built_on: thread::current().id(),
                    in_use: Cell::new(false),
                    record: &record,
                })
            },
            |state, (index, &(work_us, work, _))| {
                if state.built_on != thread::current().id() {
                    record.broke(format!("the state for item {index} moved threads"));
                }
                if state.in_use.replace(true) {
                    record.broke(format!("the state for item {index} was already in use"));
                }
                thread::sleep(Duration::from_micros(work_us));
                // On a pool thread, so the current pool is this one.
                let used = use_pool(&pool, Caller::Inside, work);
                state.in_use.set(false);
                (index, used)
            },
        )
        .try_for_each(|(index, worked)| {
            let consume = items.get(index).map_or(Use::Nothing, |&(_, _, consume)| consume);
            consumed.push((index, worked, use_pool(&pool, *caller, consume)));
            Ok(())
        })
    };
    let result = match caller {
        Caller::Outside => call(),
        Caller::Inside => pool.install(call),
    };
    let live_after = record.live.load(Ordering::SeqCst);
    Outcome { result, consumed, live_after, record }
}

fn run_with_deadline(scenario: &Scenario) -> Outcome {
    let scenario = scenario.clone();
    with_deadline(move || run(&scenario))
}

/// Runs `f` on its own thread: its result, its panic resumed here, or a
/// panic saying it hung.
fn with_deadline<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, outcome) = mpsc::channel();
    thread::spawn(move || {
        // Nobody is listening any more once the deadline has passed.
        done.send(panic::catch_unwind(AssertUnwindSafe(f))).ok();
    });
    match outcome.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(value)) => value,
        Ok(Err(payload)) => panic::resume_unwind(payload),
        Err(_) => panic!("hung"),
    }
}

/// Every item's results, in order, with what its uses returned.
fn assert_all_consumed(scenario: &Scenario, outcome: &Outcome) {
    let expected: Vec<_> = scenario
        .items
        .iter()
        .enumerate()
        .map(|(index, &(_, work, consume))| (index, expected(work), expected(consume)))
        .collect();
    assert_eq!(outcome.consumed, expected);
}

#[hegel::test]
fn the_consumer_can_use_the_pool(tc: TestCase) {
    let scenario = tc.draw(scenario(false, true).print_as_debug());
    let outcome = run_with_deadline(&scenario);
    assert!(matches!(outcome.result, Ok(Ok(()))), "{:?}", outcome.result);
    assert_all_consumed(&scenario, &outcome);
}

#[hegel::test]
fn the_work_can_use_the_pool(tc: TestCase) {
    let scenario = tc.draw(scenario(true, false).print_as_debug());
    let outcome = run_with_deadline(&scenario);
    assert!(matches!(outcome.result, Ok(Ok(()))), "{:?}", outcome.result);
    assert_all_consumed(&scenario, &outcome);
}

/// `init`'s state need not be `Send`, so it must stay on its thread, and
/// it is built once per thread; it may borrow, so it must be gone when the
/// call returns.
#[hegel::test]
fn a_state_stays_on_its_thread_while_the_pool_is_shared(tc: TestCase) {
    let scenario = tc.draw(scenario(true, true).print_as_debug());
    let outcome = run_with_deadline(&scenario);
    assert_eq!(outcome.record.broken.into_inner().unwrap(), Vec::<String>::new());
    assert!(outcome.record.inits.into_inner() <= scenario.threads, "`init` ran twice on a thread");
    assert_eq!(outcome.live_after, 0, "states outlived the call");
}

/// Two maps chained on one pool, the second taking the first's results as
/// its items: the second's workers wait in the first's iterator, on the
/// threads the first's workers need.
#[derive(Debug, Clone)]
struct Stages {
    threads: usize,
    windows: (usize, usize),
    caller: Caller,
    /// How long each item takes in the first stage and in the second.
    items: Vec<(u64, u64)>,
}

#[hegel::composite]
fn stages(tc: &TestCase) -> Stages {
    let threads = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(6));
    let window = || tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(8));
    let windows = (window(), window());
    let caller = if tc.draw_silent(gs::booleans()) { Caller::Inside } else { Caller::Outside };
    let items = (0..tc.draw_silent(below(60)))
        .map(|_| (tc.draw_silent(latency()), tc.draw_silent(latency())))
        .collect();
    Stages { threads, windows, caller, items }
}

#[hegel::test]
fn stages_can_share_a_pool(tc: TestCase) {
    let stages = tc.draw(stages().print_as_debug());
    let Stages { threads, windows: (first, second), caller, items } = stages.clone();
    let result = with_deadline(move || {
        let pool = pool(threads);
        let run = || {
            let first = in_order(items.iter().enumerate()).window(first);
            let first = match caller {
                Caller::Outside => first.pool(&pool),
                Caller::Inside => first,
            };
            first
                .map(|(index, &(us, next_us))| {
                    thread::sleep(Duration::from_micros(us));
                    (index, next_us)
                })
                .with_iter(|firsts| {
                    let second = in_order(firsts).window(second);
                    let second = match caller {
                        Caller::Outside => second.pool(&pool),
                        Caller::Inside => second,
                    };
                    second
                        .map(|(index, us)| {
                            thread::sleep(Duration::from_micros(us));
                            index
                        })
                        .with_iter(|seconds| Ok::<_, String>(seconds.collect::<Vec<_>>()))
                        .or_unwind()
                })
        };
        match caller {
            Caller::Outside => run(),
            Caller::Inside => pool.install(run),
        }
    });
    let consumed = result.unwrap().unwrap();
    assert_eq!(consumed, (0..stages.items.len()).collect::<Vec<_>>());
}
