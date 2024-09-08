use crate::metrics::Magnitude;

pub const POISONED_LOCK: &str = "poisoned lock";

pub const GENERAL_BYTES_BUCKETS: &[Magnitude] =
    &[0.0, 1024.0, 4096.0, 16384.0, 65536.0, 1.0 * 1024.0 * 1024.0];

pub const GENERAL_HIGH_PRECISION_SECONDS_BUCKETS: &[Magnitude] =
    &[0.0001, 0.001, 0.01, 0.1, 1.0, 10.0];

// The low precision clock cannot distinguish <20ms values, so we just have one bucket for those,
// as they are essentially "zero duration".
pub const GENERAL_LOW_PRECISION_SECONDS_BUCKETS: &[Magnitude] = &[0.02, 0.5, 1.0, 5.0, 10.0];
