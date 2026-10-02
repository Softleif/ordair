use crate::{Error, WorkerError, in_order};
use rayon::ThreadPool;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Duration,
};

mod properties;

fn pool(threads: usize) -> ThreadPool {
    rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap()
}

#[test]
fn results_arrive_in_input_order_behind_a_slow_item() {
    let mut seen = Vec::new();
    in_order(0..500_usize)
        .pool(&pool(8))
        .window(4)
        .map(|i| {
            if i % 97 == 0 {
                thread::sleep(Duration::from_millis(20));
            }
            i
        })
        .try_for_each(|i| {
            seen.push(i);
            Ok::<_, String>(())
        })
        .unwrap();
    assert_eq!(seen, (0..500).collect::<Vec<_>>());
}

#[test]
fn workers_never_run_more_than_the_window_ahead() {
    let started = AtomicUsize::new(0);
    let mut consumed = 0;
    in_order(0..200_usize)
        .pool(&pool(8))
        .window(3)
        .map(|i| {
            started.fetch_add(1, Ordering::SeqCst);
            if i == 0 {
                thread::sleep(Duration::from_millis(50));
            }
        })
        .try_for_each(|()| {
            // The window plus the one the consumer just took off the queue.
            let ahead = started.load(Ordering::SeqCst) - consumed;
            assert!(ahead <= 3 + 1, "{ahead} items started ahead of the consumer");
            consumed += 1;
            Ok::<_, String>(())
        })
        .unwrap();
    assert_eq!(consumed, 200);
}

#[test]
fn the_default_window_is_four_per_thread() {
    let started = AtomicUsize::new(0);
    let mut consumed = 0;
    in_order(0..200_usize)
        .pool(&pool(2))
        .map(|i| {
            started.fetch_add(1, Ordering::SeqCst);
            if i == 0 {
                thread::sleep(Duration::from_millis(50));
            }
        })
        .try_for_each(|()| {
            let ahead = started.load(Ordering::SeqCst) - consumed;
            assert!(ahead <= 2 * 4 + 1, "{ahead} items started ahead of the consumer");
            consumed += 1;
            Ok::<_, String>(())
        })
        .unwrap();
    assert_eq!(consumed, 200);
}

#[test]
#[should_panic(expected = "must not be zero")]
fn the_window_cannot_be_zero() {
    let _ = in_order(0..1).window(0);
}

#[test]
fn runs_on_the_current_pool_by_default() {
    let mut threads = Vec::new();
    pool(3)
        .install(|| {
            in_order(0..50).map(|_| rayon::current_num_threads()).try_for_each(|n| {
                threads.push(n);
                Ok::<_, String>(())
            })
        })
        .unwrap();
    assert_eq!(threads, [3; 50]);
}

#[test]
fn state_is_built_once_per_worker() {
    let built = AtomicUsize::new(0);
    in_order(0..100)
        .pool(&pool(4))
        .window(8)
        .map_init(|| Ok(built.fetch_add(1, Ordering::SeqCst)), |_, i| i)
        .try_for_each(|_| Ok::<_, String>(()))
        .unwrap();
    assert!((1..=4).contains(&built.load(Ordering::SeqCst)));
}

#[test]
fn a_thread_without_items_builds_no_state() {
    let built = AtomicUsize::new(0);
    in_order(0..1)
        .pool(&pool(8))
        .map_init(|| Ok(built.fetch_add(1, Ordering::SeqCst)), |_, i| i)
        .try_for_each(|_| Ok::<_, String>(()))
        .unwrap();
    assert_eq!(built.load(Ordering::SeqCst), 1);
}

#[test]
fn a_consumer_error_stops_the_workers() {
    let started = AtomicUsize::new(0);
    let result = in_order(0..10_000)
        .pool(&pool(4))
        .window(2)
        .map(|i| {
            started.fetch_add(1, Ordering::SeqCst);
            i
        })
        .try_for_each(|i| if i == 10 { Err("disk full".to_owned()) } else { Ok(()) });
    assert_eq!(result.unwrap_err().to_string(), "disk full");
    assert!(started.load(Ordering::SeqCst) < 100);
}

#[test]
fn an_init_error_is_returned_after_the_taken_items_are_consumed() {
    let attempts = AtomicUsize::new(0);
    let mut consumed = Vec::new();
    let result = in_order(0..1_000)
        .pool(&pool(2))
        .window(4)
        .map_init(
            || {
                // The first worker to get here fails; the other may already
                // be working.
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err("cannot open the BAM".to_owned())
                } else {
                    thread::sleep(Duration::from_millis(10));
                    Ok(())
                }
            },
            |(), i| i,
        )
        .try_for_each(|i| {
            consumed.push(i);
            Ok(())
        });
    assert_eq!(result.unwrap_err().to_string(), "cannot open the BAM");
    assert!(consumed.len() < 1_000);
    assert_eq!(consumed, (0..consumed.len()).collect::<Vec<_>>());
}

#[test]
fn a_panicking_worker_is_an_error_not_a_gap() {
    let mut consumed = Vec::new();
    let result = in_order(0..100)
        .pool(&pool(4))
        .window(4)
        .map(|i| {
            assert_ne!(i, 30, "boom");
            i
        })
        .try_for_each(|i| {
            consumed.push(i);
            Ok::<_, String>(())
        });
    assert!(matches!(&result, Err(Error::Worker(WorkerError::Panicked(_)))));
    assert!(result.unwrap_err().to_string().contains("boom"));
    assert_eq!(consumed, (0..30).collect::<Vec<_>>());
}

#[test]
fn a_worker_and_a_consumer_failing_are_both_returned() {
    let attempts = AtomicUsize::new(0);
    let result = in_order(0..2)
        .pool(&pool(2))
        .window(4)
        .map_init(
            || match attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(()),
                _ => Err("init failed".to_owned()),
            },
            // Long enough for the second worker to take the second item
            |(), i| thread::sleep(Duration::from_millis(100 * (1 - i))),
        )
        .try_for_each(|()| Err("consume failed".to_owned()));
    match result {
        Err(Error::Both { worker: WorkerError::Init(worker), consumer }) => {
            assert_eq!((worker.as_str(), consumer.as_str()), ("init failed", "consume failed"));
        }
        other => panic!("expected both failures, got {other:?}"),
    }
}

#[test]
fn can_be_called_from_a_thread_of_its_pool() {
    let (done, finished) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let pool = pool(4);
        let mut seen = Vec::new();
        let result = pool.install(|| {
            in_order(0..100).pool(&pool).window(4).map(|i| i).try_for_each(|i| {
                seen.push(i);
                Ok::<_, String>(())
            })
        });
        done.send((result.is_ok(), seen)).ok();
    });
    let (ok, seen) = finished.recv_timeout(Duration::from_secs(5)).expect("hung");
    assert!(ok);
    assert_eq!(seen, (0..100).collect::<Vec<_>>());
}

#[test]
#[should_panic(expected = "only thread of its pool")]
fn the_only_thread_of_its_pool_cannot_call_it() {
    let pool = pool(1);
    pool.install(|| in_order(0..1).pool(&pool).map(|i| i).try_for_each(|_| Ok::<_, String>(())))
        .ok();
}

#[test]
#[should_panic(expected = "only thread of its pool")]
fn the_only_thread_of_the_current_pool_cannot_call_it() {
    pool(1).install(|| in_order(0..1).map(|i| i).try_for_each(|_| Ok::<_, String>(()))).ok();
}
