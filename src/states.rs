//! The state `init` builds, one per thread of the pool.
//!
//! A worker gives its thread back whenever the window is full, and the
//! worker that replaces it may run on any thread. The state has to stay
//! behind on its thread, as it need not be `Send`, so it lives here, in the
//! slot for that thread's index, until a broadcast drops it there.

use std::{
    cell::{Cell, UnsafeCell},
    mem,
    sync::atomic::{AtomicBool, Ordering},
};

pub(crate) struct States<S> {
    slots: Box<[Slot<S>]>,
    any_built: AtomicBool,
}

struct Slot<S> {
    state: UnsafeCell<Option<S>>,
    /// Whether a [`Claim`] on it is alive.
    claimed: Cell<bool>,
}

// SAFETY: a slot is only ever touched through `claim` and `drop_mine`, which
// pick it by `rayon_core::current_thread_index()`. Workers and the broadcast
// in `drop_mine` run on the threads of the pool the slots were made for, as
// rayon only ever runs a pool's jobs on that pool's own threads, so each slot
// is only touched by the one thread with its index: `S` never moves
// threads and the `Cell` is never shared. That thread can come back to the
// slot further down its own stack, by stealing a worker while one is waiting
// in `work`; `claimed` turns that second claim away.
unsafe impl<S> Sync for States<S> {}

impl<S> States<S> {
    /// One slot per thread of the pool.
    pub(crate) fn new(threads: usize) -> Self {
        let slots = (0..threads)
            .map(|_| Slot { state: UnsafeCell::new(None), claimed: Cell::new(false) })
            .collect();
        Self { slots, any_built: AtomicBool::new(false) }
    }

    /// This thread's slot, unless it is claimed further up the stack.
    pub(crate) fn claim(&self) -> Option<Claim<'_, S>> {
        let slot = self.slots.get(rayon_core::current_thread_index()?)?;
        if slot.claimed.replace(true) {
            return None;
        }
        Some(Claim(slot))
    }

    /// Records that a state was built, so that [`drop_mine`](Self::drop_mine)
    /// needs to run.
    pub(crate) fn built<'s>(&self, state: &'s mut S) -> &'s mut S {
        self.any_built.store(true, Ordering::SeqCst);
        state
    }

    /// Whether any state was built that needs to be dropped on its thread.
    pub(crate) fn any_built(&self) -> bool {
        mem::needs_drop::<S>() && self.any_built.load(Ordering::SeqCst)
    }

    /// Drops this thread's state.
    pub(crate) fn drop_mine(&self) {
        if let Some(mut claim) = self.claim() {
            drop(claim.get().take());
        }
    }
}

impl<S> Drop for States<S> {
    /// A state still here was never dropped on its own thread, which only a
    /// bug in ordair would cause; dropping it here could break what `S`
    /// relies on by not being `Send`, so it leaks.
    fn drop(&mut self) {
        for slot in &mut self.slots {
            #[allow(clippy::mem_forget, reason = "see above")]
            mem::forget(slot.state.get_mut().take());
        }
    }
}

/// A thread's own slot, claimed by a worker running there.
pub(crate) struct Claim<'a, S>(&'a Slot<S>);

impl<S> Claim<'_, S> {
    pub(crate) fn get(&mut self) -> &mut Option<S> {
        // SAFETY: this is the only claim on the slot, as `claimed` says, and
        // it is on the slot's own thread, as `States::claim` picked it by
        // that thread's index and `Claim` is neither `Send` nor `Sync`.
        unsafe { &mut *self.0.state.get() }
    }
}

impl<S> Drop for Claim<'_, S> {
    fn drop(&mut self) {
        self.0.claimed.set(false);
    }
}
