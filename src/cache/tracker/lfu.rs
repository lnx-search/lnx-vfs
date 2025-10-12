use std::collections::VecDeque;
use std::hash::BuildHasher;
use std::ops::Range;

use hashbrown::hash_table::{self, HashTable};

use super::frequency_sketch::FrequencySketch;
use crate::cache::{LayerId, PageIndex};

const FREQ_SLOTS: usize = 256;

macro_rules! remove_lookup {
    ($lookup:expr, $hash_key:expr, $eq_check:expr, $hasher:expr $(,)?) => {{
        let maybe_occupied = $lookup.entry($hash_key, $eq_check, $hasher);
        if let hash_table::Entry::Occupied(entry) = maybe_occupied {
            let (idx, _) = entry.remove();
            Some(idx)
        } else {
            None
        }
    }};
}

/// A LFU tracker that works in a similar concept to an LFU cache except for we are not really
/// using it to hold the data, just to track the status of pages and who we should and should
/// not allow into the cache.
pub struct LfuCacheTracker {
    capacity: usize,
    hasher: foldhash::fast::RandomState,

    frequency_sketch: FrequencySketch,
    frequency_counts: FrequencyCounts,

    lookup: HashTable<u32>,

    first_node: u32,
    last_node: u32,
    entries: slab::Slab<Node<Entry>>,

    evicted_entries: VecDeque<(LayerId, PageIndex)>,
}

impl LfuCacheTracker {
    /// Create a new [LfuCacheTracker] with capacity for a given number of pages.
    pub fn with_capacity(num_pages: usize) -> Self {
        let hasher = foldhash::fast::RandomState::default();
        let mut frequency_sketch = FrequencySketch::default();
        frequency_sketch.ensure_capacity(num_pages as u32);
        let frequency_counts = FrequencyCounts::default();
        let lookup = HashTable::with_capacity(num_pages);
        let entries = slab::Slab::with_capacity(num_pages);

        Self {
            capacity: num_pages,
            hasher,
            frequency_sketch,
            frequency_counts,
            lookup,
            first_node: u32::MAX,
            last_node: u32::MAX,
            entries,
            evicted_entries: VecDeque::new(),
        }
    }

    /// Register an "access" to the given span of pages for the given [LayerId].
    pub fn insert(&mut self, layer_id: LayerId, page_range: Range<PageIndex>) {
        for page in page_range {
            let hash_key = self.hasher.hash_one((layer_id, page));

            self.frequency_sketch.increment(hash_key);
            let new_freq = self.frequency_sketch.frequency(hash_key);

            // Always allow entries if the cache isn't full.
            if self.lookup.len() >= self.capacity
                && new_freq < self.frequency_counts.min_freq()
            {
                self.evicted_entries.push_back((layer_id, page));
                continue;
            }

            let eq_check = |idx: &u32| {
                let entry = &self.entries[*idx as usize].entry;
                entry.layer_id == layer_id && entry.page == page
            };
            let hasher = |idx: &u32| self.entries[*idx as usize].entry.hash_key;
            let idx = *self
                .lookup
                .entry(hash_key, eq_check, hasher)
                .or_insert_with(|| {
                    self.frequency_counts.register_freq(new_freq);
                    self.entries.insert(Node {
                        entry: Entry {
                            hash_key,
                            layer_id,
                            page,
                            freq: new_freq,
                        },
                        next_node: u32::MAX,
                        prev_node: u32::MAX,
                    }) as u32
                })
                .get();

            let entry = &mut self.entries[idx as usize].entry;
            if entry.freq != new_freq {
                self.frequency_counts.register_freq(new_freq);
                self.frequency_counts.unregister_freq(entry.freq);
                entry.freq = new_freq;
            }

            self.update_nodes(idx);
        }

        let remove_n = self.lookup.len().saturating_sub(self.capacity);
        self.remove_tail_n(remove_n);
    }

    /// Remove all entries from the cache within the given page range for a given layer.
    ///
    /// If `drop_evictions` is `true`, this will not move the removed entries into the
    /// `evicted_entries` queue.
    pub fn remove(
        &mut self,
        layer_id: LayerId,
        page_range: Range<PageIndex>,
        drop_evictions: bool,
    ) {
        for page in page_range {
            let hash_key = self.hasher.hash_one((layer_id, page));

            let hasher = |idx: &u32| self.entries[*idx as usize].entry.hash_key;

            let eq_check = |idx: &u32| {
                let entry = &self.entries[*idx as usize].entry;
                entry.layer_id == layer_id && entry.page == page
            };

            let Some(idx) = remove_lookup!(self.lookup, hash_key, eq_check, hasher)
            else {
                continue;
            };

            self.remove_at(idx, drop_evictions);
        }
    }

    /// Returns a reference to the queue of evicted entries.
    pub fn evicted_entries(&mut self) -> &mut VecDeque<(LayerId, PageIndex)> {
        &mut self.evicted_entries
    }

    fn update_nodes(&mut self, idx: u32) {
        if idx == self.first_node {
            return;
        }

        // Update our existing head to point to our new head.
        if self.first_node != u32::MAX {
            let old_head = &mut self.entries[self.first_node as usize];
            old_head.prev_node = idx;
        }

        let target_head = &mut self.entries[idx as usize];
        let right_node = target_head.next_node;
        let left_node = target_head.prev_node;

        // Update our tail pointer, if our right node is `u32::MAX` then we know we
        // are the tail and therefore need to update it.
        if idx == self.last_node {
            self.last_node = left_node;
        } else if self.last_node == u32::MAX {
            self.last_node = idx;
        }

        // Splice the gap to the right of our target
        if right_node != u32::MAX {
            let right = &mut self.entries[right_node as usize];
            right.prev_node = left_node;
        }

        // Splice the gap to the left of our target
        if left_node != u32::MAX {
            let left = &mut self.entries[left_node as usize];
            left.next_node = right_node;
        }

        // Update the new head
        let new_head = &mut self.entries[idx as usize];
        new_head.next_node = self.first_node;
        new_head.prev_node = u32::MAX;

        self.first_node = idx;
    }

    fn remove_tail_n(&mut self, n: usize) {
        for _ in 0..n {
            // This should never panic as we always update the last node
            // before calling this method.
            let remove_idx = self.last_node;
            let node = self.entries.remove(remove_idx as usize);
            self.last_node = node.prev_node;

            let removed_entry = node.entry;
            self.evicted_entries
                .push_back((removed_entry.layer_id, removed_entry.page));

            let hasher = |idx: &u32| {
                if *idx == remove_idx {
                    removed_entry.hash_key
                } else {
                    self.entries[*idx as usize].entry.hash_key
                }
            };

            let eq_check = |idx: &u32| *idx == remove_idx;

            remove_lookup!(self.lookup, node.entry.hash_key, eq_check, hasher,);
            self.frequency_counts.unregister_freq(node.entry.freq);
        }

        if n > 0 && self.last_node != u32::MAX {
            self.entries[self.last_node as usize].next_node = u32::MAX;
        }
    }

    fn remove_at(&mut self, pos: u32, drop_evictions: bool) {
        let node = self.entries.remove(pos as usize);
        self.frequency_counts.unregister_freq(node.entry.freq);

        if node.next_node != u32::MAX {
            self.entries[node.next_node as usize].prev_node = node.prev_node;
        }

        if node.prev_node != u32::MAX {
            self.entries[node.prev_node as usize].next_node = node.next_node;
        }

        if drop_evictions {
            self.evicted_entries
                .push_back((node.entry.layer_id, node.entry.page));
        }
    }
}

#[derive(Debug)]
struct FrequencyCounts {
    freq_distribution: Box<[u32; FREQ_SLOTS]>,
    min_freq: u8,
}

impl Default for FrequencyCounts {
    fn default() -> Self {
        Self {
            freq_distribution: Box::new([0; FREQ_SLOTS]),
            min_freq: u8::MAX,
        }
    }
}

impl FrequencyCounts {
    fn min_freq(&self) -> u8 {
        self.min_freq
    }

    fn register_freq(&mut self, freq: u8) {
        let pos = freq as usize;
        self.freq_distribution[pos] += 1;
        if freq < self.min_freq {
            self.min_freq = freq;
        }
    }

    fn unregister_freq(&mut self, freq: u8) {
        let pos = freq as usize;
        self.freq_distribution[pos] -= 1;
        if self.freq_distribution[pos] == 0 && self.min_freq == freq {
            self.calc_min_freq();
        }
    }

    fn calc_min_freq(&mut self) {
        for i in 0..FREQ_SLOTS {
            if self.freq_distribution[i] > 0 {
                self.min_freq = i as u8;
                return;
            }
        }
        self.min_freq = u8::MAX;
    }
}

#[derive(Copy, Clone, Debug)]
struct Entry {
    hash_key: u64,
    layer_id: LayerId,
    page: PageIndex,
    freq: u8,
}

#[derive(Copy, Clone, Debug)]
struct Node<T> {
    next_node: u32,
    prev_node: u32,
    entry: T,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freq_dist_count() {
        let mut tracker = FrequencyCounts::default();
        tracker.register_freq(4);
        assert_eq!(tracker.min_freq, 4);
        assert_eq!(tracker.freq_distribution[4], 1);

        tracker.register_freq(5);
        tracker.register_freq(4);
        assert_eq!(tracker.min_freq, 4);
        assert_eq!(tracker.freq_distribution[4], 2);
        assert_eq!(tracker.freq_distribution[5], 1);

        tracker.register_freq(2);
        assert_eq!(tracker.min_freq, 2);
        assert_eq!(tracker.freq_distribution[2], 1);

        tracker.unregister_freq(2);
        assert_eq!(tracker.min_freq, 4);
        assert_eq!(tracker.freq_distribution[2], 0);
        assert_eq!(tracker.freq_distribution[4], 2);
    }

    #[test]
    fn test_freq_tracker_inserts() {
        let mut tracker = LfuCacheTracker::with_capacity(5);

        tracker.insert(1, 0..5);
        assert!(tracker.evicted_entries.is_empty());
        assert_eq!(tracker.first_node, 4);
        assert_eq!(tracker.last_node, 0);
        assert_eq!(tracker.entries.len(), 5);
        assert_eq!(tracker.lookup.len(), 5);

        tracker.insert(2, 0..2);
        let entries = tracker.evicted_entries().drain(..).collect::<Vec<_>>();
        assert_eq!(entries, &[(1, 0), (1, 1)]);

        tracker.insert(1, 0..5);
        tracker.insert(1, 0..5);
        tracker.evicted_entries().clear();

        tracker.insert(2, 0..2);
        let entries = tracker.evicted_entries().drain(..).collect::<Vec<_>>();
        assert_eq!(entries, &[(2, 0), (2, 1)]);
    }

    #[test]
    fn test_freq_tracker_remove_range() {
        let mut tracker = LfuCacheTracker::with_capacity(5);

        tracker.insert(1, 0..5);
        assert!(tracker.evicted_entries.is_empty());
        assert_eq!(tracker.first_node, 4);
        assert_eq!(tracker.last_node, 0);
        assert_eq!(tracker.entries.len(), 5);
        assert_eq!(tracker.lookup.len(), 5);

        tracker.remove(1, 3..5, true);
        let entries = tracker.evicted_entries().drain(..).collect::<Vec<_>>();
        assert_eq!(entries, &[(1, 3), (1, 4)]);

        tracker.remove(1, 0..3, false);
        assert!(tracker.evicted_entries().is_empty());
    }
}
