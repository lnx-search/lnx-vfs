mod frequency_sketch;
mod lfu;

use std::sync::Arc;

pub use lfu::LfuCacheTracker;
use parking_lot::Mutex;
pub type SharedLfuCacheTracker = Arc<Mutex<LfuCacheTracker>>;
