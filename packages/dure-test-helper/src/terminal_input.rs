/// Sample VT input containing focus-in and an SGR mouse-button event.
///
/// The coordinates are arbitrary distinct values so a rearranged or truncated
/// sequence cannot accidentally match the fixture.
pub const SAMPLE_TERMINAL_INPUT: &[u8] = b"\x1b[I\x1b[<0;5;7M";

/// Marker printed after the helper has enabled and is ready for terminal input.
pub const TERMINAL_INPUT_READY: &str = "terminal-input-ready";

/// Marker printed after the helper receives the sample input unchanged.
pub const TERMINAL_INPUT_OK: &str = "terminal-input-ok";

/// Marker printed after the helper has discarded pre-existing input records.
pub const RESIZE_READY: &str = "resize-ready";
