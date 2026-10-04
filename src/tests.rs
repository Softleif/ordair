use crate::{OrUnwind, Panic, in_order};
use rayon_core::ThreadPool;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Duration,
};

mod properties;
mod sharing;

fn pool(threads: usize) -> ThreadPool {
    rayon_core::ThreadPoolBuilder::new().num_threads(threads).build().unwrap()
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
        .unwrap()
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
        .unwrap()
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
        .unwrap()
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
            in_order(0..50).map(|_| rayon_core::current_num_threads()).try_for_each(|n| {
                threads.push(n);
                Ok::<_, String>(())
            })
        })
        .unwrap()
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
        .unwrap()
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
        .unwrap()
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
    assert_eq!(result.unwrap(), Err("disk full".to_owned()));
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
    assert_eq!(result.unwrap(), Err("cannot open the BAM".to_owned()));
    assert!(consumed.len() < 1_000);
    assert_eq!(consumed, (0..consumed.len()).collect::<Vec<_>>());
}

#[test]
fn a_panicking_worker_is_the_outer_error_not_a_gap() {
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
    assert!(result.unwrap_err().to_string().contains("boom"));
    assert_eq!(consumed, (0..30).collect::<Vec<_>>());
}

#[test]
fn a_panicking_consumer_is_the_outer_error() {
    let mut consumed = 0;
    let result = in_order(0..1_000).pool(&pool(4)).window(4).map(|i| i).try_for_each(|i| {
        assert_ne!(i, 30, "boom");
        consumed += 1;
        Ok::<_, String>(())
    });
    let message = result.unwrap_err().to_string();
    assert!(
        message.starts_with("The consumer panicked: ") && message.contains("boom"),
        "{message}"
    );
    // Code after the call runs, e.g. to close what `consume` wrote to
    assert_eq!(consumed, 30);
}

#[test]
#[should_panic(expected = "boom")]
fn or_unwind_resumes_a_worker_panic() {
    let _ = in_order(0..10)
        .pool(&pool(2))
        .map(|i| assert_ne!(i, 3, "boom"))
        .try_for_each(|()| Ok::<_, String>(()))
        .or_unwind();
}

#[test]
fn a_consumer_error_wins_over_an_init_error() {
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
    // The consumer failed on the first item, the second worker's `init` on
    // the second, so the consumer's error is what a sequential loop returns.
    assert_eq!(result.unwrap(), Err("consume failed".to_owned()));
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
        done.send((matches!(result, Ok(Ok(()))), seen)).ok();
    });
    let (ok, seen) = finished.recv_timeout(Duration::from_secs(5)).expect("hung");
    assert!(ok);
    assert_eq!(seen, (0..100).collect::<Vec<_>>());
}

/// The calling thread runs the workers itself while it waits.
#[test]
fn the_only_thread_of_its_pool_can_call_it() {
    let (done, finished) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let pool = pool(1);
        let seen = pool.install(|| {
            in_order(0..100)
                .pool(&pool)
                .window(4)
                .map(|i| i)
                .with_iter(|results| Ok::<_, String>(results.collect::<Vec<_>>()))
        });
        done.send(seen.unwrap().unwrap()).ok();
    });
    let seen = finished.recv_timeout(Duration::from_secs(5)).expect("hung");
    assert_eq!(seen, (0..100).collect::<Vec<_>>());
}

#[test]
fn the_only_thread_of_the_current_pool_can_call_it() {
    let (done, finished) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let sum = pool(1).install(|| {
            in_order(0..100).map(|i| i).with_iter(|results| Ok::<_, String>(results.sum::<i32>()))
        });
        done.send(sum.unwrap().unwrap()).ok();
    });
    assert_eq!(finished.recv_timeout(Duration::from_secs(5)).expect("hung"), 4950);
}

#[test]
fn with_iter_returns_what_the_closure_returns() {
    let words = ["a", "bb", "ccc"];
    let result = in_order(&words)
        .pool(&pool(4))
        .map(|word| word.len())
        .with_iter(|lengths| Ok::<_, String>(lengths.zip(words).collect::<Vec<_>>()));
    assert_eq!(result.unwrap().unwrap(), [(1, "a"), (2, "bb"), (3, "ccc")]);
}

#[test]
fn stopping_the_iterator_early_stops_the_workers() {
    let started = AtomicUsize::new(0);
    let result = in_order(0..10_000)
        .pool(&pool(4))
        .window(2)
        .map(|i| {
            started.fetch_add(1, Ordering::SeqCst);
            i
        })
        .with_iter(|numbers| Ok::<_, String>(numbers.take_while(|&i| i < 10).sum::<i32>()));
    assert_eq!(result.unwrap(), Ok(45));
    assert!(started.load(Ordering::SeqCst) < 100);
}

#[test]
fn a_dropped_iterator_stops_the_workers_before_the_closure_returns() {
    let started = AtomicUsize::new(0);
    let result = in_order(0..10_000)
        .pool(&pool(4))
        .window(2)
        .map(|i| {
            started.fetch_add(1, Ordering::SeqCst);
            i
        })
        .with_iter(|mut numbers| {
            let first = numbers.next();
            drop(numbers);
            // Long enough for the workers to stop, not to run through the
            // items.
            thread::sleep(Duration::from_millis(50));
            Ok::<_, String>((first, started.load(Ordering::SeqCst)))
        });
    let (first, started_by_then) = result.unwrap().unwrap();
    assert_eq!(first, Some(0));
    assert!(started_by_then < 100, "{started_by_then}");
    assert_eq!(started.load(Ordering::SeqCst), started_by_then);
}

#[test]
fn a_forgotten_iterator_does_not_hang() {
    let (done, finished) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let result = in_order(0..10_000).pool(&pool(4)).window(2).map(|i| i).with_iter(|numbers| {
            #[allow(clippy::mem_forget, reason = "what this tests")]
            std::mem::forget(numbers);
            Ok::<_, String>(())
        });
        done.send(result.unwrap()).ok();
    });
    assert_eq!(finished.recv_timeout(Duration::from_secs(5)).expect("hung"), Ok(()));
}

#[test]
fn an_init_error_ends_the_iterator_and_is_returned() {
    let attempts = AtomicUsize::new(0);
    let result = in_order(0..1_000)
        .pool(&pool(2))
        .window(4)
        .map_init(
            || {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err("cannot open the BAM".to_owned())
                } else {
                    thread::sleep(Duration::from_millis(10));
                    Ok(())
                }
            },
            |(), i| i,
        )
        // Ends early, and the closure cannot tell.
        .with_iter(|numbers| Ok(numbers.count()));
    assert_eq!(result.unwrap(), Err("cannot open the BAM".to_owned()));
}

#[test]
fn an_init_error_past_where_the_closure_stopped_is_dropped() {
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
            |(), i| {
                thread::sleep(Duration::from_millis(100 * (1 - i)));
                i
            },
        )
        .with_iter(|mut numbers| Ok(numbers.next()));
    assert_eq!(result.unwrap(), Ok(Some(0)));
}

#[test]
fn a_worker_panic_ends_the_iterator_and_is_the_outer_error() {
    let mut consumed = Vec::new();
    let result = in_order(0..100)
        .pool(&pool(4))
        .window(4)
        .map(|i| {
            assert_ne!(i, 30, "boom");
            i
        })
        .with_iter(|numbers| {
            consumed.extend(numbers);
            Ok::<_, String>(())
        });
    assert!(result.unwrap_err().to_string().contains("boom"));
    assert_eq!(consumed, (0..30).collect::<Vec<_>>());
}

#[test]
fn a_panic_in_the_closure_is_the_outer_error() {
    let result =
        in_order(0..1_000).pool(&pool(4)).window(4).map(|i| i).with_iter(
            |mut numbers| -> Result<(), String> { panic!("boom at {:?}", numbers.next()) },
        );
    let message = result.unwrap_err().to_string();
    assert_eq!(message, "The consumer panicked: boom at Some(0)");
}
