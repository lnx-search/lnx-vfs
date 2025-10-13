use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use parking_lot::Mutex;

const APPROX_TICKETS_PER_GENERATION: u64 = 256;

/// The generation handler keeps track of active references
/// to pages over time.
///
/// The page cache uses a generational GC-like system.
///
/// Every operation increments a counter which we call a "ticket", then when we want to free a page,
/// we first increment the counter then wait until all operations that came _before_ that
/// operation have completed and ended.
///
/// Every 256 operations, we create a new generation which is a ref-count of the last 256 tickets.
/// Once a generation ref-count has dropped to zero, we advance our `oldest_alive_ticket` by 256 operations.
///
/// This method is designed to minimise contention and additional cleanup cycles while still
/// having relatively low memory overhead.
pub(super) struct GenerationTicketMachine {
    /// The monotonic ticket ID counter.
    ///
    /// This increments with every operation performed.
    ticket_counter: AtomicU64,
    /// The current generation in use for new tickets.
    ///
    /// The accesses to this do not need to be strongly consistent,
    /// meaning newer ops are allowed to get a reference to an older generation,
    /// all it means is it defers at what point the oldest alive ticket
    /// is advanced slightly.
    active_generation: ArcSwap<GenerationEntry>,
    /// State shared with [TicketGuard]s so they can automatically
    /// advance the generation state when dropped.
    shared_state: Arc<SharedState>,
}

impl Default for GenerationTicketMachine {
    fn default() -> Self {
        let shared_state = Arc::new(SharedState {
            oldest_alive_ticket: AtomicU64::new(0),
            generation: Mutex::new(GenerationState {
                known_active_tickets: BTreeSet::new(),
            }),
        });

        // This entry is immediately replaced, always.
        let base_entry = GenerationEntry {
            oldest_ticket: 0,
            shared_state: shared_state.clone(),
        };

        let this = Self {
            ticket_counter: AtomicU64::new(0),
            active_generation: ArcSwap::from_pointee(base_entry),
            shared_state,
        };
        // We always force a rotation to fix the one possible divergence which is the
        // first "oldest_ticket" the ticket machine will see, will be 1 instead of 0.
        this.increment_ticket_id();
        this
    }
}

impl GenerationTicketMachine {
    /// Returns the oldest alive ticket still in use.
    ///
    /// It can be assumed that any ticket older than this value
    /// has now been dropped.
    pub(super) fn oldest_alive_ticket(&self) -> u64 {
        self.shared_state.oldest_alive_ticket()
    }

    /// Increment the ticket counter but do not return a [TicketGuard].
    pub(super) fn increment_ticket_id(&self) -> u64 {
        let ticket_id = self.ticket_counter.fetch_add(1, Ordering::Relaxed);

        if ticket_id.is_multiple_of(APPROX_TICKETS_PER_GENERATION) {
            self.advance_generation();
        }

        ticket_id
    }

    /// Replace the current active generation with the next generation.
    pub(super) fn advance_generation(&self) {
        // Note that it is safe to increment the ticket counter here, even if we are
        // advancing the generation because we hit the `APPROX_TICKETS_PER_GENERATION`
        // threshold.
        //
        // This is because how we calculate the oldest ticket is respective of the generation
        // ID and is instead solely based on the `oldest_ticket` defined within the generation
        // which is this value we load.
        //
        // This means even if we force an advance on ticket 255 and the next increment
        // hits 256 and rotates the generation again, we do not end up in a situation where
        // the oldest alive ticket is ever larger than the possibly alive tickets.
        //
        // That being said, double advances can be a little bit inefficient, but I don't believe
        // it will be a large issue and can always be optimised by not forcing an advance
        // as much.
        let oldest_ticket = self.ticket_counter.fetch_add(1, Ordering::Relaxed);
        let generation = self.shared_state.replace_generation(oldest_ticket);
        self.active_generation.store(generation);
    }

    /// Increments the ticket operation counter and returns a [TicketGuard]
    /// which will ensure the oldest ticket counter is not advanced until
    /// this guard is dropped.
    pub(super) fn get_next_ticket(&self) -> TicketGuard {
        let generation = self.active_generation.load_full();
        self.increment_ticket_id();
        TicketGuard { generation }
    }
}

/// A guard that represents a single operation and its lifecycle.
///
/// This is used to ensure mutations to pages are not applied until
/// all readers still accessing the page have been dropped.
pub(super) struct TicketGuard {
    generation: Arc<GenerationEntry>,
}

impl TicketGuard {
    /// Clones `self` for use in reads.
    ///
    /// This is a separate method to prevent accidental misuse.
    pub(super) fn clone_for_read(&self) -> Self {
        Self {
            generation: self.generation.clone(),
        }
    }
}

struct SharedState {
    /// The oldest ticket ID still alive.
    ///
    /// This is an estimated value based on the generations currently alive.
    /// However, this value cannot lead to false-negatives, meaning newer
    /// ticket IDs may not be alive, but it is guaranteed that all tickets
    /// older than this ID are no longer alive.
    oldest_alive_ticket: AtomicU64,
    /// Generation state used for tracking ticket lifecycles.
    generation: Mutex<GenerationState>,
}

impl SharedState {
    fn oldest_alive_ticket(&self) -> u64 {
        self.oldest_alive_ticket.load(Ordering::Relaxed)
    }

    fn set_oldest_alive_ticket(&self, ticket: u64) {
        self.oldest_alive_ticket.store(ticket, Ordering::Relaxed);
    }

    fn replace_generation(self: &Arc<Self>, oldest_ticket: u64) -> Arc<GenerationEntry> {
        let shared_state = self.clone();
        let mut lock = self.generation.lock();

        let entry = GenerationEntry {
            oldest_ticket,
            shared_state,
        };

        lock.push_active_generation(entry)
    }
}

struct GenerationState {
    known_active_tickets: BTreeSet<u64>,
}

impl GenerationState {
    fn push_active_generation(
        &mut self,
        entry: GenerationEntry,
    ) -> Arc<GenerationEntry> {
        let entry = Arc::new(entry);
        self.known_active_tickets.insert(entry.oldest_ticket);
        entry
    }
}

/// A generation that holds a reference back to its shared state.
///
/// When this entry is dropped it will remove itself from the `active_generations`
/// state.
struct GenerationEntry {
    oldest_ticket: u64,
    shared_state: Arc<SharedState>,
}

impl Drop for GenerationEntry {
    fn drop(&mut self) {
        let mut guard = self.shared_state.generation.lock();
        guard.known_active_tickets.remove(&self.oldest_ticket);

        // The oldest generation ID is at the head of the BTree.
        let oldest_alive_ticket = guard
            .known_active_tickets
            .first()
            .copied()
            .unwrap_or(u64::MAX);
        self.shared_state
            .set_oldest_alive_ticket(oldest_alive_ticket);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_state_ptr_chain_updates_oldest_ticket() {
        let shared_state = Arc::new(SharedState {
            oldest_alive_ticket: AtomicU64::new(0),
            generation: Mutex::new(GenerationState {
                known_active_tickets: BTreeSet::new(),
            }),
        });

        let gen1 = shared_state.replace_generation(0);
        assert_eq!(gen1.oldest_ticket, 0);
        let gen2 = shared_state.replace_generation(1);
        assert_eq!(gen2.oldest_ticket, 1);
        let gen3 = shared_state.replace_generation(2);
        assert_eq!(gen3.oldest_ticket, 2);

        drop(gen2);

        // gen1 should still be taken as the oldest generation and therefore the
        // oldest ticket must be `0`.
        assert_eq!(shared_state.oldest_alive_ticket(), 0);

        drop(gen1);
        // now gen1 is dropped, the oldest alive ticket should become the start of
        // the gen3 tickets.
        assert_eq!(shared_state.oldest_alive_ticket(), 2);

        let gen4 = shared_state.replace_generation(3);
        drop(gen3);
        assert_eq!(shared_state.oldest_alive_ticket(), 3);
        drop(gen4);
    }

    #[test]
    fn test_ticket_machine_advance_generation() {
        let machine = GenerationTicketMachine::default();

        let guard = machine.get_next_ticket();

        // This should be 1 because the first fetch_add will return 0,
        // then rotate the generation as a result and increment the ticket again
        // resulting in 1.
        // The ticket ID for the guard would be 2.
        assert_eq!(guard.generation.oldest_ticket, 1);

        machine.advance_generation();
        drop(guard);

        let guard = machine.get_next_ticket();
        assert_eq!(guard.generation.oldest_ticket, 3);
        assert_eq!(machine.oldest_alive_ticket(), 3);

        let ticket = machine.increment_ticket_id();
        assert_eq!(ticket, 5);
    }

    #[test]
    fn test_ticket_machine_increment() {
        let machine = GenerationTicketMachine::default();

        assert_eq!(machine.increment_ticket_id(), 2);
        assert_eq!(machine.increment_ticket_id(), 3);
        assert_eq!(machine.increment_ticket_id(), 4);

        assert_eq!(machine.oldest_alive_ticket(), 1);

        for _ in 0..256 {
            machine.increment_ticket_id();
        }
        assert_eq!(machine.oldest_alive_ticket(), 257);

        for _ in 0..256 {
            machine.increment_ticket_id();
        }
        assert_eq!(machine.oldest_alive_ticket(), 513);
    }

    #[test]
    fn test_threaded_ticket_machine_increment() {
        let machine = Arc::new(GenerationTicketMachine::default());

        let handle1 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        let handle2 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(machine.oldest_alive_ticket(), 2049);
    }

    #[test]
    fn test_ticket_guard_prevent_oldest_advancing() {
        let machine = GenerationTicketMachine::default();
        assert_eq!(machine.increment_ticket_id(), 2);

        let guard = machine.get_next_ticket();

        for _ in 0..256 {
            machine.increment_ticket_id();
        }
        assert_eq!(machine.oldest_alive_ticket(), 1);

        drop(guard);
        assert_eq!(machine.oldest_alive_ticket(), 257);
    }

    #[test]
    fn test_threaded_ticket_guard_prevent_oldest_advancing() {
        let machine = Arc::new(GenerationTicketMachine::default());
        assert_eq!(machine.increment_ticket_id(), 2);

        let guard = machine.get_next_ticket();

        let handle1 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        let handle2 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
        assert_eq!(machine.oldest_alive_ticket(), 1);

        drop(guard);
        assert_eq!(machine.oldest_alive_ticket(), 2049);
    }
}
