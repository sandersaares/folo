use crate::metrics::Magnitude;

pub const POISONED_LOCK: &str = "poisoned lock";

pub const GENERAL_BYTES_BUCKETS: &[Magnitude] = &[0, 1024, 4096, 16384, 65536, 1024 * 1024];

// The low precision clock cannot distinguish <20ms values, so we just have one bucket for those,
// as they can all be considered essentially "infinitesimal duration".
pub const GENERAL_MILLISECONDS_BUCKETS: &[Magnitude] = &[20, 500, 1000, 5000, 10000];
