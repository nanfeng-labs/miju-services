//! The gate word: one bit for `closed`, the rest for the lease count.
//!
//! A lease is granted only by a successful compare-and-swap on a word whose
//! `closed` bit is clear and whose count is below capacity. Once the word is
//! `closed` with a zero count it stays that way, so nothing can be admitted
//! after a drain has been observed.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};

const CLOSED: u64 = 1 << 63;

/// Why admission failed. Nothing was admitted and nothing ran.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Rejected {
    /// The service is stopping or its root has finished.
    #[error("admission is closed")]
    Closed,
    /// Queued, running and leased responsibility occupy every slot.
    #[error("capacity is exhausted")]
    Full,
}

#[derive(Debug)]
pub(crate) struct Gate {
    word: AtomicU64,
    capacity: u64,
}

impl Gate {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            word: AtomicU64::new(0),
            capacity: capacity.min(CLOSED - 1),
        }
    }

    /// Take one lease. This is the only admission decision.
    pub(crate) fn admit(&self) -> Result<(), Rejected> {
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            if current & CLOSED != 0 {
                return Err(Rejected::Closed);
            }
            if current >= self.capacity {
                return Err(Rejected::Full);
            }
            match self.word.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Close admission. True if this call moved the gate into the drained state.
    pub(crate) fn close(&self) -> bool {
        self.word.fetch_or(CLOSED, Ordering::AcqRel) == 0
    }

    /// Return one lease. True if this call moved the gate into the drained state.
    pub(crate) fn release(&self) -> bool {
        self.word.fetch_sub(1, Ordering::AcqRel) == CLOSED | 1
    }

    pub(crate) fn count(&self) -> u64 {
        self.word.load(Ordering::Acquire) & !CLOSED
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.word.load(Ordering::Acquire) & CLOSED != 0
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.word.load(Ordering::Acquire) == CLOSED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_closure_and_one_drain_signal() {
        let gate = Gate::new(2);
        assert_eq!(gate.admit(), Ok(()));
        assert_eq!(gate.admit(), Ok(()));
        assert_eq!(gate.admit(), Err(Rejected::Full));
        assert!(!gate.release());
        assert!(!gate.close(), "one lease is still held");
        assert_eq!(gate.admit(), Err(Rejected::Closed));
        assert!(!gate.is_drained());
        assert!(gate.release(), "the last release reports the drain");
        assert!(gate.is_drained());
        assert!(!gate.close(), "a second close reports nothing");
        assert!(
            Gate::new(1).close(),
            "closing an empty gate reports the drain"
        );
    }
}
