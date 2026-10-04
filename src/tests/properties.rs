//! Runs random pools, windows, latencies and failures against what the
//! caller is promised, whatever the interleaving.

use super::*;
use hegel::{Generator, TestCase, generators as gs};
use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicIsize},
        mpsc,
    },
};

#[derive(Debug, Clone, Copy)]
enum Failure {
    Error,
    Panic,
}

/// How the closure of `with_iter` leaves the iterator when it stops early.
#[derive(Debug, Clone, Copy)]
enum Stop {
    Return,
    /// Drops it, then takes its time before returning.
    Drop,
    Forget,
}

/// How the results are consumed.
#[derive(Debug, Clone, Copy)]
enum Api {
    TryForEach,
    /// A `for` loop over `with_iter`'s iterator, which stops once that many
    /// items are consumed, if it gets that far.
    WithIter(Option<(usize, Stop)>),
}

#[derive(Debug, Clone)]
struct Item {
    work_us: u64,
    consume_us: u64,
    panics: bool,
}

#[derive(Debug, Clone)]
struct Scenario {
    threads: usize,
    window: usize,
    items: Vec<Item>,
    /// The n-th call to `init` (in time, not by thread) fails.
    init_fails: Option<(usize, Failure)>,
    /// The n-th call to `consume` fails.
    consume_fails: Option<(usize, Failure)>,
    /// The n-th call to the iterator's `next` panics; `items.len()` is
    /// the call that would have ended it.
    next_panics: Option<usize>,
    api: Api,
}

fn below(n: usize) -> impl Generator<usize> {
    gs::integers::<usize>().max_value(n - 1)
}

/// Mostly no latency, so that most cases are about interleaving.
#[hegel::composite]
fn latency(tc: &TestCase) -> u64 {
    if tc.draw_silent(gs::weighted_booleans(0.2)) {
        tc.draw_silent(gs::integers::<u64>().max_value(499))
    } else {
        0
    }
}

#[hegel::composite]
fn item(tc: &TestCase) -> Item {
    Item {
        work_us: tc.draw_silent(latency()),
        consume_us: tc.draw_silent(latency()),
        panics: tc.draw_silent(gs::weighted_booleans(0.02)),
    }
}

fn failure() -> impl Generator<Failure> {
    gs::sampled_from(&[Failure::Error, Failure::Panic])
}

// Failures are drawn among the calls that happen, so that none is
// wasted on a call past the end.
#[hegel::composite]
fn scenario(tc: &TestCase) -> Scenario {
    let threads = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(6));
    let window = tc.draw_silent(gs::integers::<usize>().min_value(1).max_value(8));
    // hegel keeps a vector's own length short, and long ones are where
    // the window fills up.
    let len = tc.draw_silent(below(60));
    let items = (0..len).map(|_| tc.draw_silent(item())).collect();
    let init_fails = tc
        .draw_silent(gs::weighted_booleans(0.15))
        .then(|| (tc.draw_silent(below(threads)), tc.draw_silent(failure())));
    let consume_fails = (len > 0 && tc.draw_silent(gs::weighted_booleans(0.15)))
        .then(|| (tc.draw_silent(below(len)), tc.draw_silent(failure())));
    let next_panics =
        tc.draw_silent(gs::weighted_booleans(0.1)).then(|| tc.draw_silent(below(len + 1)));
    let api = if tc.draw_silent(gs::booleans()) {
        Api::TryForEach
    } else {
        let stop = tc.draw_silent(gs::weighted_booleans(0.5)).then(|| {
            let how = tc.draw_silent(gs::sampled_from(&[Stop::Return, Stop::Drop, Stop::Forget]));
            (tc.draw_silent(below(len + 1)), how)
        });
        Api::WithIter(stop)
    };
    Scenario { threads, window, items, init_fails, consume_fails, next_panics, api }
}

/// Counts live values, so a result or state that is never dropped
/// shows up as a leak.
struct Tracked<'a>(&'a AtomicIsize);

impl<'a> Tracked<'a> {
    fn new(live: &'a AtomicIsize) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self(live)
    }
}

impl Drop for Tracked<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What the run did, recorded as it happened.
#[derive(Default)]
struct Record {
    inits: AtomicUsize,
    /// Items the iterator returned.
    pulled: AtomicUsize,
    /// `consume` calls that returned `Ok`.
    done: AtomicUsize,
    live: AtomicIsize,
    worker_panicked: AtomicBool,
    init_failed: AtomicBool,
    consume_failed: AtomicBool,
    consume_panicked: AtomicBool,
    /// `with_iter`'s closure stopped before the iterator ended.
    stopped: AtomicBool,
    broken: Mutex<Vec<String>>,
}

impl Record {
    fn broke(&self, promise: String) {
        self.broken.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(promise);
    }
}

struct Outcome {
    /// `Err` when `try_for_each` unwound.
    result: thread::Result<Result<Result<(), String>, Panic>>,
    consumed: Vec<usize>,
    record: Record,
}

fn run(scenario: &Scenario) -> Outcome {
    let Scenario { threads, window, items, init_fails, consume_fails, next_panics, api } = scenario;
    let record = Record::default();
    let mut consumed = Vec::new();

    let mut calls = 0;
    let mut ended = false;
    let source = std::iter::from_fn(|| {
        let call = calls;
        calls += 1;
        if ended {
            record.broke(format!("`next` called again after call {}", call - 1));
        }
        // A worker takes its place in the window before the item.
        let done = record.done.load(Ordering::SeqCst);
        if call > done + window {
            record.broke(format!("item {call} pulled when {done} were consumed"));
        }
        if *next_panics == Some(call) {
            ended = true;
            record.worker_panicked.store(true, Ordering::SeqCst);
            panic!("next panicked");
        }
        let item = items.get(call);
        ended = item.is_none();
        if item.is_some() {
            record.pulled.fetch_add(1, Ordering::SeqCst);
        }
        item.map(|item| (call, item))
    });

    let mut consume = |(index, consume_us, _result): (usize, u64, Tracked<'_>)| {
        match consume_fails {
            Some((n, Failure::Error)) if *n == consumed.len() => {
                record.consume_failed.store(true, Ordering::SeqCst);
                return Err("consume failed".to_owned());
            }
            Some((n, Failure::Panic)) if *n == consumed.len() => {
                record.consume_panicked.store(true, Ordering::SeqCst);
                panic!("consume panicked");
            }
            _ => {}
        }
        thread::sleep(Duration::from_micros(consume_us));
        consumed.push(index);
        record.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let pool = pool(*threads);
        let map = in_order(source).pool(&pool).window(*window).map_init(
            || {
                let nth = record.inits.fetch_add(1, Ordering::SeqCst);
                match init_fails {
                    Some((n, Failure::Error)) if *n == nth => {
                        record.init_failed.store(true, Ordering::SeqCst);
                        Err("init failed".to_owned())
                    }
                    Some((n, Failure::Panic)) if *n == nth => {
                        record.worker_panicked.store(true, Ordering::SeqCst);
                        panic!("init panicked")
                    }
                    _ => Ok((thread::current().id(), Tracked::new(&record.live))),
                }
            },
            |(built_on, _), (index, item): (usize, &Item)| {
                if *built_on != thread::current().id() {
                    record.broke(format!("the state for item {index} moved threads"));
                }
                let done = record.done.load(Ordering::SeqCst);
                if index > done + window {
                    record.broke(format!("item {index} started when {done} were consumed"));
                }
                thread::sleep(Duration::from_micros(item.work_us));
                if item.panics {
                    record.worker_panicked.store(true, Ordering::SeqCst);
                    panic!("item {index} panicked");
                }
                (index, item.consume_us, Tracked::new(&record.live))
            },
        );
        let Api::WithIter(stop) = api else { return map.try_for_each(consume) };
        map.with_iter(|mut results| {
            loop {
                if let Some((k, how)) = stop
                    && record.done.load(Ordering::SeqCst) == *k
                {
                    record.stopped.store(true, Ordering::SeqCst);
                    match how {
                        Stop::Return => {}
                        Stop::Drop => {
                            drop(results);
                            thread::sleep(Duration::from_micros(200));
                        }
                        #[allow(clippy::mem_forget, reason = "what this tests")]
                        Stop::Forget => std::mem::forget(results),
                    }
                    return Ok(());
                }
                let Some(result) = results.next() else { return Ok(()) };
                consume(result)?;
            }
        })
    }));
    Outcome { result, consumed, record }
}

/// A hang is the failure this module most needs to catch, so every
/// case runs on its own thread with a deadline.
fn run_with_deadline(scenario: &Scenario) -> Option<Outcome> {
    let (done, outcome) = mpsc::channel();
    let scenario = scenario.clone();
    thread::spawn(move || {
        // Nobody is listening any more once the deadline has passed.
        done.send(run(&scenario)).ok();
    });
    outcome.recv_timeout(Duration::from_secs(5)).ok()
}

fn message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

fn check(scenario: &Scenario) {
    let Some(Outcome { result, consumed, record }) = run_with_deadline(scenario) else {
        panic!("hung")
    };
    let Scenario { threads, items, init_fails, consume_fails, next_panics, api, .. } = scenario;
    let stop = match api {
        Api::WithIter(Some((k, _))) => Some(*k),
        _ => None,
    };
    let n = items.len();
    let pulled = record.pulled.into_inner();
    let worker_panicked = record.worker_panicked.into_inner();
    let init_failed = record.init_failed.into_inner();
    let consume_failed = record.consume_failed.into_inner();
    let consume_panicked = record.consume_panicked.into_inner();
    let stopped = record.stopped.into_inner();

    // Whatever happens: every promise kept along the way, an in-order
    // prefix, nothing leaked, and no worker built its state twice.
    assert_eq!(record.broken.into_inner().unwrap(), Vec::<String>::new());
    assert_eq!(consumed, (0..consumed.len()).collect::<Vec<_>>());
    assert_eq!(record.live.into_inner(), 0, "results or states leaked");
    assert!(record.inits.into_inner() <= *threads, "`init` called more than once per worker");
    let first_panic = items.iter().position(|item| item.panics);
    let consume_fails = consume_fails.map(|(f, _)| f);
    let limit = [first_panic, consume_fails, *next_panics, stop].into_iter().flatten().min();
    assert!(consumed.len() <= limit.unwrap_or(n).min(n));

    // Nothing unwinds out of the call. A panic in `consume` wins, as it
    // comes from an earlier item than any worker's, then a worker panic,
    // then the consumer's error, then a failed `init`'s, but only if the
    // results got as far as the item it lost.
    let worker_failed = worker_panicked || init_failed;
    match &result {
        Err(payload) => panic!("unwound with {:?}", message(&**payload)),
        Ok(Err(panic)) if consume_panicked => {
            assert_eq!(panic.to_string(), "The consumer panicked: consume panicked");
        }
        Ok(_) if consume_panicked => panic!("a panic in `consume` went unreported"),
        Ok(Err(panic)) => assert!(worker_panicked, "returned {panic:?}"),
        Ok(Ok(_)) if worker_panicked => panic!("a worker panic went unreported"),
        Ok(Ok(Ok(()))) if stopped => {
            assert!(!consume_failed, "a failure went unreported");
            assert_eq!(Some(consumed.len()), stop);
        }
        Ok(Ok(Ok(()))) => {
            assert!(!init_failed && !consume_failed, "a failure went unreported");
            assert_eq!(consumed.len(), n);
        }
        Ok(Ok(Err(error))) if consume_failed => assert_eq!(error, "consume failed"),
        Ok(Ok(Err(error))) => {
            assert!(init_failed && !stopped, "returned {error:?}");
            assert_eq!(error, "init failed");
        }
    }

    // With a single failure, there is only one right outcome; stopping
    // early only cuts it short.
    let upto = |end: usize| end.min(stop.unwrap_or(n));
    match (init_fails, first_panic, consume_fails, next_panics) {
        (None, None, None, None) => assert_eq!(consumed.len(), upto(n)),
        (None, Some(p), None, None) => assert_eq!(consumed.len(), upto(p)),
        (None, None, Some(f), None) => assert_eq!(consumed.len(), upto(f)),
        (None, None, None, Some(k)) if stop.is_none() => {
            assert_eq!((consumed.len(), pulled), (*k, *k));
        }
        (None, None, None, Some(k)) => assert_eq!(consumed.len(), upto(*k)),
        // `init` runs on a worker's first item, which is lost when it
        // fails, so the results stop before it, unless they stopped first
        (Some(_), None, None, None) if worker_failed && !stopped => {
            assert!(consumed.len() < pulled);
        }
        // The failing call never came: fewer workers got an item
        (Some(_), None, None, None) if !worker_failed => assert_eq!(consumed.len(), upto(n)),
        _ => {}
    }
}

#[hegel::test]
fn behaves_as_promised(tc: TestCase) {
    check(&tc.draw(scenario().print_as_debug()));
}

/// A worker that stops abnormally must close the queue, or the consumer
/// waits forever. proptest first found this with the only worker
/// panicking in `init`; since `init` runs on a worker's first item, that
/// case ends at the lost item, and the iterator panicking is what is left.
#[test]
fn the_only_worker_panicking_does_not_hang() {
    check(&Scenario {
        threads: 1,
        window: 1,
        items: Vec::new(),
        init_fails: None,
        consume_fails: None,
        next_panics: Some(0),
        api: Api::TryForEach,
    });
}
