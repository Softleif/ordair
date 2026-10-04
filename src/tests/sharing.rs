//! The map shares its pool: the items, the work and the consumer may all
//! use it too, and maps may be chained on it, whatever the pool's size, the
//! window, or where the call is made from.
//!
//! "It never hangs" is the contract under test; a hang is caught by
//! running every case on its own thread with a deadline. A case that hangs
//! leaves its threads behind, which only costs memory.

use super::*;
use hegel::{Generator, TestCase, generators as gs};
use std::{
    cell::Cell,
    collections::HashMap,
    panic::{self, AssertUnwindSafe},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicIsize},
        mpsc,
    },
    thread::ThreadId,
};

/// Something done with the map's pool.
#[derive(Debug, Clone, Copy)]
enum Use {
    Nothing,
    /// `install` a closure that does nothing.
    Install,
    /// Two halves of a `join`, each taking this long.
    Join(u64),
    /// Run a closure on every thread of the pool.
    Broadcast,
    /// Another map on the same pool, over `0..len`, with a state that needs
    /// dropping, consuming the first `take` results if given, else all.
    Nested {
        len: usize,
        window: usize,
        take: Option<usize>,
    },
}

/// Where the map is called from.
#[derive(Debug, Clone, Copy)]
enum Caller {
    /// A thread outside any pool, with `.pool(&pool)`.
    Outside,
    /// One of the pool's own threads, with the current pool.
    Inside,
    /// A thread of another pool of that many threads, with `.pool(&pool)`.
    OtherPool(usize),
    /// A thread outside any pool, on the global pool.
    Global,
}

/// The one thing that goes wrong, if any.
#[derive(Debug, Clone, Copy)]
enum Failure {
    /// The work panics on that item.
    WorkPanics(usize),
    /// The n-th call to `init`, in time, fails.
    InitFails(usize),
    /// The consumer stops after that many results.
    Stops(usize),
}

#[derive(Debug, Clone)]
struct Item {
    work_us: u64,
    /// Done with the pool by the items iterator, the work and the consumer.
    by_items: Use,
    by_work: Use,
    by_consumer: Use,
}

#[derive(Debug, Clone)]
struct Scenario {
    threads: usize,
    window: usize,
    caller: Caller,
    items: Vec<Item>,
    failure: Option<Failure>,
}

/// Which of the three use the pool in a property.
#[derive(Clone, Copy)]
struct Users {
    items: bool,
    work: bool,
    consumer: bool,
}

const ALL: Users = Users { items: true, work: true, consumer: true };
const NONE: Users = Users { items: false, work: false, consumer: false };

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

#[hegel::composite]
fn caller(tc: &TestCase) -> Caller {
    match tc.draw_silent(below(4)) {
        0 => Caller::Outside,
        1 => Caller::Inside,
        2 => Caller::OtherPool(tc.draw_silent(below(3)) + 1),
        _ => Caller::Global,
    }
}

fn scenario(users: Users, failures: bool) -> impl Generator<Scenario> {
    hegel::compose!(|tc| {
        let threads = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(6));
        let window = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(8));
        let caller = tc.draw_silent(caller());
        // Long enough for the window to fill.
        let len = tc.draw_silent(below(40));
        let use_if = |uses| if uses { tc.draw_silent(pool_use()) } else { Use::Nothing };
        let items = (0..len)
            .map(|_| Item {
                by_items: use_if(users.items),
                by_work: use_if(users.work),
                by_consumer: use_if(users.consumer),
                work_us: tc.draw_silent(latency()),
            })
            .collect();
        let failure = (failures && len > 0).then(|| match tc.draw_silent(below(3)) {
            0 => Failure::WorkPanics(tc.draw_silent(below(len))),
            1 => Failure::InitFails(tc.draw_silent(below(threads))),
            _ => Failure::Stops(tc.draw_silent(below(len))),
        });
        Scenario { threads, window, caller, items, failure }
    })
}

/// The pool a use goes to: the map's, or the current one, which is the
/// global pool outside any.
#[derive(Clone, Copy)]
enum Target<'a> {
    Pool(&'a ThreadPool),
    Current,
}

/// Uses the pool as `what` says and returns a value to check: the sum of
/// the results consumed from a nested map, else zero.
fn use_pool(target: Target<'_>, what: Use) -> usize {
    let sleep = |us| move || thread::sleep(Duration::from_micros(us));
    match (what, target) {
        (Use::Nothing, _) => 0,
        (Use::Install, Target::Pool(pool)) => pool.install(|| 0),
        (Use::Install, Target::Current) => rayon_core::scope(|_| 0),
        (Use::Join(us), Target::Pool(pool)) => {
            pool.install(|| rayon_core::join(sleep(us), sleep(us)));
            0
        }
        (Use::Join(us), Target::Current) => {
            rayon_core::join(sleep(us), sleep(us));
            0
        }
        (Use::Broadcast, Target::Pool(pool)) => {
            pool.broadcast(|_| ());
            0
        }
        (Use::Broadcast, Target::Current) => {
            rayon_core::broadcast(|_| ());
            0
        }
        (Use::Nested { len, window, take }, target) => {
            let nested = in_order(0..len).window(window);
            let nested = match target {
                Target::Pool(pool) => nested.pool(pool),
                Target::Current => nested,
            };
            nested
                .map_init(|| Ok(String::from("state")), |state, i| i + state.len() - 5)
                .with_iter(|results| Ok::<_, String>(results.take(take.unwrap_or(len)).sum()))
                .or_unwind()
                .unwrap()
        }
    }
}

/// What a use returns, by the same rules.
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
    /// States built, by thread.
    built: Mutex<HashMap<ThreadId, usize>>,
    live: AtomicIsize,
    init_failed: AtomicBool,
    broken: Mutex<Vec<String>>,
}

impl Record {
    fn broke(&self, promise: String) {
        self.broken.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(promise);
    }
}

type Consumed = (usize, usize, usize, usize);

struct Outcome {
    result: Result<Result<(), String>, Panic>,
    /// For each consumed item, its index and what its uses returned.
    consumed: Vec<Consumed>,
    /// How many states were alive when the call returned.
    live_after: isize,
    record: Record,
}

fn run(scenario: &Scenario) -> Outcome {
    let Scenario { threads, window, caller, items, failure } = scenario;
    let pool = pool(*threads);
    let target = match caller {
        Caller::Global => Target::Current,
        _ => Target::Pool(&pool),
    };
    let record = Record::default();
    let mut consumed = Vec::new();
    let mut call = || {
        let items = items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, item, use_pool(target, item.by_items)));
        let map = in_order(items).window(*window);
        let map = match caller {
            Caller::Outside | Caller::OtherPool(_) => map.pool(&pool),
            Caller::Inside | Caller::Global => map,
        };
        map.map_init(
            || {
                let nth = record.inits.fetch_add(1, Ordering::SeqCst);
                if matches!(failure, Some(Failure::InitFails(n)) if *n == nth) {
                    record.init_failed.store(true, Ordering::SeqCst);
                    return Err("init failed".to_owned());
                }
                let mut built = record.built.lock().unwrap();
                *built.entry(thread::current().id()).or_default() += 1;
                record.live.fetch_add(1, Ordering::SeqCst);
                let built_on = thread::current().id();
                Ok(State { built_on, in_use: Cell::new(false), record: &record })
            },
            |state, (index, item, by_items): (usize, &Item, usize)| {
                if state.built_on != thread::current().id() {
                    record.broke(format!("the state for item {index} moved threads"));
                }
                if state.in_use.replace(true) {
                    record.broke(format!("the state for item {index} was already in use"));
                }
                thread::sleep(Duration::from_micros(item.work_us));
                if matches!(failure, Some(Failure::WorkPanics(k)) if *k == index) {
                    state.in_use.set(false);
                    panic!("work panicked");
                }
                let by_work = use_pool(target, item.by_work);
                state.in_use.set(false);
                (index, by_items, by_work, item.by_consumer)
            },
        )
        .with_iter(|results| {
            for (index, by_items, by_work, by_consumer) in results {
                if matches!(failure, Some(Failure::Stops(k)) if *k == consumed.len()) {
                    break;
                }
                consumed.push((index, by_items, by_work, use_pool(target, by_consumer)));
            }
            Ok(())
        })
    };
    let result = match caller {
        Caller::Outside | Caller::Global => call(),
        Caller::Inside => pool.install(call),
        Caller::OtherPool(threads) => self::pool(*threads).install(call),
    };
    let live_after = record.live.load(Ordering::SeqCst);
    Outcome { result, consumed, live_after, record }
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

fn run_with_deadline(scenario: &Scenario) -> Outcome {
    let scenario = scenario.clone();
    with_deadline(move || run(&scenario))
}

/// What the first `n` items should have returned.
fn expected_prefix(scenario: &Scenario, n: usize) -> Vec<Consumed> {
    scenario
        .items
        .iter()
        .take(n)
        .enumerate()
        .map(|(index, item)| {
            (index, expected(item.by_items), expected(item.by_work), expected(item.by_consumer))
        })
        .collect()
}

/// Every item's result, in order, with what its uses returned.
fn assert_all_consumed(scenario: &Scenario, outcome: &Outcome) {
    assert!(matches!(outcome.result, Ok(Ok(()))), "{:?}", outcome.result);
    assert_eq!(outcome.consumed, expected_prefix(scenario, scenario.items.len()));
}

#[hegel::test]
fn the_consumer_can_use_the_pool(tc: TestCase) {
    let users = Users { consumer: true, ..NONE };
    let scenario = tc.draw(scenario(users, false).print_as_debug());
    assert_all_consumed(&scenario, &run_with_deadline(&scenario));
}

#[hegel::test]
fn the_work_can_use_the_pool(tc: TestCase) {
    let users = Users { work: true, ..NONE };
    let scenario = tc.draw(scenario(users, false).print_as_debug());
    assert_all_consumed(&scenario, &run_with_deadline(&scenario));
}

#[hegel::test]
fn the_items_can_use_the_pool(tc: TestCase) {
    let users = Users { items: true, ..NONE };
    let scenario = tc.draw(scenario(users, false).print_as_debug());
    assert_all_consumed(&scenario, &run_with_deadline(&scenario));
}

/// A failure stops the map as documented, with the pool in use all around.
#[hegel::test]
fn a_failure_stops_the_map_while_the_pool_is_shared(tc: TestCase) {
    let scenario = tc.draw(scenario(ALL, true).print_as_debug());
    let outcome = run_with_deadline(&scenario);
    let len = scenario.items.len();
    match scenario.failure {
        None => assert_all_consumed(&scenario, &outcome),
        Some(Failure::WorkPanics(k)) => {
            let message = outcome.result.as_ref().unwrap_err().to_string();
            assert_eq!(message, "A worker panicked: work panicked");
            assert_eq!(outcome.consumed, expected_prefix(&scenario, k));
        }
        Some(Failure::InitFails(_)) if outcome.record.init_failed.load(Ordering::SeqCst) => {
            assert_eq!(outcome.result.as_ref().unwrap(), &Err("init failed".to_owned()));
            let n = outcome.consumed.len();
            assert!(n < len, "the item whose `init` failed was consumed");
            assert_eq!(outcome.consumed, expected_prefix(&scenario, n));
        }
        // The failing call never came: fewer threads got an item.
        Some(Failure::InitFails(_)) => assert_all_consumed(&scenario, &outcome),
        Some(Failure::Stops(k)) => {
            assert!(matches!(outcome.result, Ok(Ok(()))), "{:?}", outcome.result);
            assert_eq!(outcome.consumed, expected_prefix(&scenario, k));
        }
    }
}

/// `init`'s state need not be `Send`, so it must stay on its thread, and
/// it is built once per thread; it may borrow, so it must be gone when the
/// call returns.
#[hegel::test]
fn a_state_stays_on_its_thread_while_the_pool_is_shared(tc: TestCase) {
    let scenario = tc.draw(scenario(ALL, true).print_as_debug());
    let outcome = run_with_deadline(&scenario);
    assert_eq!(outcome.record.broken.into_inner().unwrap(), Vec::<String>::new());
    let built = outcome.record.built.into_inner().unwrap();
    assert!(built.values().all(|&n| n == 1), "`init` ran twice on a thread: {built:?}");
    assert_eq!(outcome.live_after, 0, "states outlived the call");
}

/// Two maps chained on one pool, the second taking the first's results as
/// its items, while each one's work uses the pool too.
#[derive(Debug, Clone)]
struct Stages {
    threads: usize,
    windows: (usize, usize),
    caller: Caller,
    /// For each item, how long it takes and what it does with the pool, in
    /// the first stage and in the second.
    items: Vec<((u64, Use), (u64, Use))>,
}

#[hegel::composite]
fn stages(tc: &TestCase) -> Stages {
    let threads = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(6));
    let window = || tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(8));
    let windows = (window(), window());
    let caller = tc.draw_silent(caller());
    let stage = || (tc.draw_silent(latency()), tc.draw_silent(pool_use()));
    let items = (0..tc.draw_silent(below(40))).map(|_| (stage(), stage())).collect();
    Stages { threads, windows, caller, items }
}

#[hegel::test]
fn stages_can_share_a_pool(tc: TestCase) {
    let stages = tc.draw(stages().print_as_debug());
    let Stages { threads, windows: (first, second), caller, items } = stages.clone();
    let result = with_deadline(move || {
        let pool = pool(threads);
        let target = match caller {
            Caller::Global => Target::Current,
            _ => Target::Pool(&pool),
        };
        let work = |(us, what): (u64, Use)| {
            thread::sleep(Duration::from_micros(us));
            use_pool(target, what)
        };
        let run = || {
            let firsts = in_order(items.iter().enumerate()).window(first);
            let firsts = match caller {
                Caller::Outside | Caller::OtherPool(_) => firsts.pool(&pool),
                Caller::Inside | Caller::Global => firsts,
            };
            firsts.map(|(index, &(stage, next))| (index, work(stage), next)).with_iter(|firsts| {
                let seconds = in_order(firsts).window(second);
                let seconds = match caller {
                    Caller::Outside | Caller::OtherPool(_) => seconds.pool(&pool),
                    Caller::Inside | Caller::Global => seconds,
                };
                seconds
                    .map(|(index, first, stage)| (index, first, work(stage)))
                    .with_iter(|seconds| Ok::<_, String>(seconds.collect::<Vec<_>>()))
                    .or_unwind()
            })
        };
        match caller {
            Caller::Outside | Caller::Global => run(),
            Caller::Inside => pool.install(run),
            Caller::OtherPool(threads) => self::pool(threads).install(run),
        }
    });
    let expected: Vec<_> = stages
        .items
        .iter()
        .enumerate()
        .map(|(index, &((_, first), (_, second)))| (index, expected(first), expected(second)))
        .collect();
    assert_eq!(result.unwrap().unwrap(), expected);
}
