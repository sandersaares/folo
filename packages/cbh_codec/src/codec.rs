//! The on-the-wire byte format for stored objects: gzip.
//!
//! Stored objects are repetitive JSON (per-object ratios of ~10–30×), and the
//! Azure backend transfers every stored byte on upload and on every `analyze`
//! read, so the storage layer compresses object bodies transparently. This module
//! is the single source of that encoding: the `cargo-bench-history` storage
//! backends call it on `put`/`get`, and the stress harness calls the *same*
//! function when it writes its synthetic tree, so the harness can never drift out
//! of lockstep with production.
//!
//! gzip (via the pure-Rust `miniz_oxide` backend) is chosen for three properties
//! this layer depends on:
//!
//! * **Determinism** — the same input always produces byte-identical output (the
//!   gzip header's mtime is zero and the OS byte is fixed), so a stress seed is
//!   reproducible and an object's bytes are a pure function of its contents.
//! * **Self-describing framing** — the gzip magic (`0x1f 0x8b`) never collides
//!   with a JSON object's first byte (`{` / whitespace), so [`decompress`] returns
//!   a clean error on a legacy uncompressed object rather than silent garbage.
//! * **Standard `Content-Encoding`** — `gzip` is a registered HTTP content
//!   coding, so the Azure backend can declare it and any non-SDK reader can
//!   inflate the blob with off-the-shelf tooling.
//!
//! # Allocation reuse
//!
//! `analyze` decompresses every stored object and the write paths compress every
//! one, so the codec runs tens of thousands of times per invocation. The
//! expensive part of a gzip round-trip is the deflate/inflate working state (the
//! match-finder hash tables and the sliding window), not the output buffer. That
//! state is therefore held in a per-thread [`Compress`]/[`Decompress`] and reset
//! between calls, so a thread allocates it once and reuses it for every object it
//! handles. The framing (the fixed gzip header, the CRC-32 and length trailer) is
//! written and parsed directly so the low-level reusable coders — which speak raw
//! DEFLATE, not gzip — can drive the hot path while the bytes on the wire stay
//! exactly the standard gzip a stock decoder expects.

use std::cell::RefCell;
use std::io::{Error, ErrorKind, Result};

use flate2::{Compress, Compression, Crc, Decompress, FlushCompress, FlushDecompress, Status};

/// The deflate level applied to every stored object.
///
/// Level 6 (`flate2`'s default) balances ratio against CPU. This data is written
/// once and read many times, so a higher level (up to 9) would be a defensible
/// tune if read-side wire volume ever dominated; level 6 already removes the bulk
/// of the redundancy at a fraction of level 9's compression cost.
const GZIP_LEVEL: u32 = 6;

/// Length of the fixed gzip member header this module writes and expects.
const HEADER_LEN: usize = 10;

/// Length of the gzip member trailer: a CRC-32 of the plaintext followed by its
/// length modulo 2³², each a little-endian `u32`.
const TRAILER_LEN: usize = 8;

/// Output window the coders fill per pass.
///
/// Each iteration hands the encoder this many bytes to write into, then copies
/// what it produced onto the output. A fixed window means progress never depends
/// on inspecting the output buffer's spare capacity, and 64 KiB clears most
/// objects in a single pass while bounding the work on the objects that need
/// several.
const SCRATCH_CHUNK: usize = 65_536;

/// The fixed 10-byte gzip member header.
///
/// The bytes are: magic (`1f 8b`), deflate compression method (`08`), no flags
/// (`00`), a zero mtime (`00 00 00 00`), no extra-flags (`00`), and an unknown OS
/// (`ff`). Holding mtime and the OS byte constant is what makes the output a pure
/// function of the input.
const GZIP_HEADER: [u8; HEADER_LEN] = [0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];

thread_local! {
    /// The reusable raw-DEFLATE compressor for this thread. `zlib_header` is
    /// `false` because this module writes the gzip framing itself.
    static DEFLATE: RefCell<Compress> =
        RefCell::new(Compress::new(Compression::new(GZIP_LEVEL), false));

    /// The reusable raw-INFLATE decompressor for this thread.
    static INFLATE: RefCell<Decompress> = RefCell::new(Decompress::new(false));

    /// Reusable output window for the coder loops, sized once and refilled each
    /// pass so neither coder allocates a fresh buffer per object.
    static SCRATCH: RefCell<Vec<u8>> = RefCell::new(vec![0; SCRATCH_CHUNK]);
}

/// gzip-compresses `plain` for storage.
///
/// Infallible: the encoder writes into an in-memory [`Vec`], and writing to a
/// `Vec` never fails, so there is no I/O error to surface (an allocation failure
/// aborts rather than returning).
#[must_use]
pub fn compress(plain: &[u8]) -> Vec<u8> {
    DEFLATE.with_borrow_mut(|deflate| {
        SCRATCH.with_borrow_mut(|scratch| {
            // Compressed output is rarely larger than the input plus framing;
            // reserve that up front so the common case writes the whole body
            // without a regrow.
            let hint = plain
                .len()
                .saturating_add(HEADER_LEN)
                .saturating_add(TRAILER_LEN)
                .saturating_add(16);
            let mut out = Vec::with_capacity(hint);
            out.extend_from_slice(&GZIP_HEADER);
            run_deflate(deflate, scratch, plain, &mut out);

            let mut crc = Crc::new();
            crc.update(plain);
            out.extend_from_slice(&crc.sum().to_le_bytes());
            out.extend_from_slice(&plaintext_isize(plain).to_le_bytes());
            out
        })
    })
}

/// Inflates a gzip-compressed object body produced by [`compress`].
///
/// # Errors
///
/// Returns an [`std::io::Error`] if `stored` is not a valid gzip stream — a
/// corrupt or truncated body, a CRC-32 or length mismatch, or a legacy
/// uncompressed object whose first bytes are not the gzip magic. Decoding never
/// silently returns partial or wrong data.
pub fn decompress(stored: &[u8]) -> Result<Vec<u8>> {
    let body = gzip_body(stored)?;

    let mut plain = Vec::new();
    INFLATE.with_borrow_mut(|inflate| {
        SCRATCH.with_borrow_mut(|scratch| run_inflate(inflate, scratch, body, &mut plain))
    })?;

    verify_trailer(stored, &plain)?;
    Ok(plain)
}

/// Validates the fixed gzip header and returns the raw-DEFLATE body slice (the
/// bytes between the header and the trailer).
fn gzip_body(stored: &[u8]) -> Result<&[u8]> {
    let Some(body_end) = stored.len().checked_sub(TRAILER_LEN) else {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "stored object is too short to be a gzip member",
        ));
    };

    // The header is matched byte-for-byte: magic, deflate method, then zero flags.
    // The arms are ordered most-specific first so each failure names its cause.
    match stored {
        [0x1f, 0x8b, 0x08, 0x00, ..] => {}
        [0x1f, 0x8b, 0x08, ..] => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "stored object sets unsupported gzip header flags",
            ));
        }
        [0x1f, 0x8b, ..] => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "stored object uses an unsupported gzip compression method",
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "stored object is not gzip (bad magic); it may be a legacy uncompressed object",
            ));
        }
    }

    // A body that ends before the header does is a trailer-only fragment: the
    // slice bounds reject it, so no separate length guard is needed.
    stored.get(HEADER_LEN..body_end).ok_or_else(|| {
        Error::new(
            ErrorKind::UnexpectedEof,
            "stored object is too short to be a gzip member",
        )
    })
}

/// Verifies the gzip trailer (CRC-32 then ISIZE) against the inflated plaintext.
fn verify_trailer(stored: &[u8], plain: &[u8]) -> Result<()> {
    let trailer_start = stored
        .len()
        .checked_sub(TRAILER_LEN)
        .expect("decompress validated the stored length before inflating");
    let trailer: [u8; TRAILER_LEN] = stored
        .get(trailer_start..)
        .and_then(|tail| <[u8; TRAILER_LEN]>::try_from(tail).ok())
        .expect("the trailer is exactly eight bytes");
    let [c0, c1, c2, c3, i0, i1, i2, i3] = trailer;
    let expected_crc = u32::from_le_bytes([c0, c1, c2, c3]);
    let expected_isize = u32::from_le_bytes([i0, i1, i2, i3]);

    let mut crc = Crc::new();
    crc.update(plain);
    if crc.sum() != expected_crc {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "stored object failed its gzip CRC-32 check",
        ));
    }
    if plaintext_isize(plain) != expected_isize {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "stored object length does not match its gzip ISIZE",
        ));
    }
    Ok(())
}

/// The gzip ISIZE field: the plaintext length modulo 2³².
#[expect(
    clippy::cast_possible_truncation,
    reason = "gzip ISIZE is defined as the input length modulo 2^32"
)]
fn plaintext_isize(plain: &[u8]) -> u32 {
    plain.len() as u32
}

/// Drives the reusable compressor to raw-DEFLATE all of `input`, appending the
/// compressed bytes to `out` (after whatever header `out` already holds).
///
/// Each pass fills `scratch` with the next slice of compressed output and copies
/// it onto `out`; `out` therefore grows only by what was actually produced, and
/// progress never depends on `out`'s spare capacity.
fn run_deflate(deflate: &mut Compress, scratch: &mut [u8], input: &[u8], out: &mut Vec<u8>) {
    deflate.reset();
    loop {
        let consumed = usize::try_from(deflate.total_in())
            .expect("a compressed body shorter than usize::MAX was processed");
        let remaining = input
            .get(consumed..)
            .expect("the deflate input cursor never passes the input end");
        let produced_before = deflate.total_out();
        let status = deflate
            .compress(remaining, scratch, FlushCompress::Finish)
            .expect("deflate into an in-memory buffer cannot fail");

        let produced = usize::try_from(
            deflate
                .total_out()
                .checked_sub(produced_before)
                .expect("the output counter is monotonic"),
        )
        .expect("an output shorter than usize::MAX was produced");
        out.extend_from_slice(
            scratch
                .get(..produced)
                .expect("a pass never produces more than the scratch window holds"),
        );

        if matches!(status, Status::StreamEnd) {
            break;
        }
    }
}

/// Drives the reusable decompressor to raw-INFLATE `body`, appending the
/// plaintext to `out`.
///
/// # Errors
///
/// Returns an [`std::io::Error`] if the deflate stream ends before its final
/// block — a truncated gzip body.
fn run_inflate(
    inflate: &mut Decompress,
    scratch: &mut [u8],
    body: &[u8],
    out: &mut Vec<u8>,
) -> Result<()> {
    inflate.reset(false);
    // The deflate body is a lower bound on the plaintext, so it sizes the output
    // conservatively: incompressible data lands without an over-reservation while
    // genuinely compressible data grows the buffer the few extra times it needs.
    out.reserve(body.len().max(64));
    loop {
        let consumed = usize::try_from(inflate.total_in())
            .expect("a compressed body shorter than usize::MAX was processed");
        let remaining = body
            .get(consumed..)
            .expect("the inflate input cursor never passes the body end");
        let produced_before = inflate.total_out();
        // The full body is available from the first call, so intermediate calls
        // use `None`: the decoder reports `StreamEnd` from the deflate final-block
        // marker regardless of flush mode, while `Finish` re-invoked across output
        // grows would have the decoder reject the now-empty input tail.
        let status = inflate
            .decompress(remaining, scratch, FlushDecompress::None)
            .map_err(|error| Error::new(ErrorKind::InvalidData, error))?;

        let produced = usize::try_from(
            inflate
                .total_out()
                .checked_sub(produced_before)
                .expect("the output counter is monotonic"),
        )
        .expect("an output shorter than usize::MAX was produced");
        out.extend_from_slice(
            scratch
                .get(..produced)
                .expect("a pass never produces more than the scratch window holds"),
        );

        if matches!(status, Status::StreamEnd) {
            return Ok(());
        }

        // The stream did not end, yet a full output window went unwritten: with
        // room to spare the decoder produced nothing, so it has run out of input
        // before the deflate final block — the gzip body is truncated.
        if produced == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "gzip body ended before the deflate stream was complete",
            ));
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::arithmetic_side_effects,
        clippy::indexing_slicing,
        reason = "panic is fine in tests"
    )]

    use std::io::{Read as _, Write as _};

    use flate2::read::GzDecoder;
    use flate2::write::GzEncoder;

    use super::*;

    /// A representative repetitive object body — the kind of JSON the storage
    /// layer actually stores, where the same field names recur on every record.
    fn sample_json() -> Vec<u8> {
        // Repetition, not volume, is the property these JSON round trips exercise.
        const RECORD_COUNT: usize = 2;

        let record = r#"{"id":["fast_time","capture","two_instants"],"metrics":[{"kind":"WallTime","value":12.5}]},"#;
        let mut body = String::from(r#"{"schema":1,"results":["#);
        for _ in 0..RECORD_COUNT {
            body.push_str(record);
        }
        body.push_str("]}");
        body.into_bytes()
    }

    /// An incompressible payload: random-looking bytes that gzip cannot shrink,
    /// so both the compress and decompress output buffers must grow past their
    /// first guess — exercising the grow path in both coder loops.
    fn incompressible() -> Vec<u8> {
        incompressible_of(200_000)
    }

    /// `len` incompressible bytes from a simple LCG, which has no redundancy gzip
    /// can exploit.
    fn incompressible_of(len: usize) -> Vec<u8> {
        let mut state = 0x1234_5678_u32;
        std::iter::repeat_with(|| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            u8::try_from(state >> 24).expect("a byte-wide shift yields a single byte")
        })
        .take(len)
        .collect()
    }

    #[test]
    fn roundtrips_representative_json() {
        let plain = sample_json();
        let restored = decompress(&compress(&plain)).unwrap();
        assert_eq!(restored, plain);
    }

    #[test]
    fn roundtrips_empty_input() {
        let restored = decompress(&compress(b"")).unwrap();
        assert_eq!(restored, b"");
    }

    #[test]
    fn roundtrips_non_utf8_bytes() {
        // The codec is byte-oriented; it must not assume its input is text.
        let plain: Vec<u8> = (0_u8..=255).cycle().take(1000).collect();
        let restored = decompress(&compress(&plain)).unwrap();
        assert_eq!(restored, plain);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "incompressible input spanning scratch windows requires a large codec workload"
    )]
    fn roundtrips_incompressible_input_growing_both_buffers() {
        let plain = incompressible();
        let compressed = compress(&plain);
        // The incompressible body spans scratch windows in both directions.
        let restored = decompress(&compressed).unwrap();
        assert_eq!(restored, plain);
    }

    #[test]
    fn roundtrips_across_small_scratch_windows() {
        // A small scratch window exercises the same multi-pass coder loops as
        // large objects, without interpreting a production-sized payload.
        const SCRATCH_LEN: usize = 8;

        let plain = incompressible_of(SCRATCH_LEN * 2);
        let mut scratch = [0; SCRATCH_LEN];
        let mut deflate = Compress::new(Compression::new(GZIP_LEVEL), false);
        let mut compressed = Vec::new();
        run_deflate(&mut deflate, &mut scratch, &plain, &mut compressed);
        assert!(compressed.len() > scratch.len());

        let mut inflate = Decompress::new(false);
        let mut restored = Vec::new();
        run_inflate(&mut inflate, &mut scratch, &compressed, &mut restored).unwrap();
        assert_eq!(restored, plain);
    }

    #[test]
    fn reuses_thread_state_across_many_calls() {
        // Consecutive distinct inputs exercise reset under Miri; native tests
        // additionally cover sustained reuse without paying that interpreter cost.
        const CALLS: u32 = if cfg!(miri) { 2 } else { 50 };
        const PAYLOAD_LEN: u32 = if cfg!(miri) { 8 } else { 1000 };

        // Repeated calls on one thread reuse the per-thread coders; each must
        // still produce an independent, correct round-trip.
        for seed in 0..CALLS {
            let plain: Vec<u8> = (0..PAYLOAD_LEN)
                .map(|i| (i ^ seed).to_le_bytes()[0])
                .collect();
            assert_eq!(decompress(&compress(&plain)).unwrap(), plain);
        }
    }

    #[test]
    fn is_deterministic() {
        let plain = sample_json();
        // Byte-identical output across calls is what makes a stress seed
        // reproducible; pin against re-compression rather than a brittle golden.
        assert_eq!(compress(&plain), compress(&plain));
    }

    #[test]
    fn emits_gzip_magic() {
        let compressed = compress(&sample_json());
        assert!(
            compressed.starts_with(&[0x1f, 0x8b]),
            "a stored body must carry the gzip magic so legacy plaintext is distinguishable"
        );
    }

    #[test]
    fn output_is_standard_gzip_readable_by_a_stock_decoder() {
        // A stock gzip decoder must inflate our output, proving the framing is
        // standard and the Azure `Content-Encoding: gzip` promise holds.
        let plain = sample_json();
        let compressed = compress(&plain);
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut restored = Vec::new();
        decoder.read_to_end(&mut restored).unwrap();
        assert_eq!(restored, plain);
    }

    #[test]
    fn decompresses_output_from_a_stock_encoder() {
        // Conversely, we must read any standard gzip a stock encoder wrote, so
        // objects stored by other gzip producers round-trip cleanly.
        let plain = sample_json();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(GZIP_LEVEL));
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();

        let restored = decompress(&compressed).unwrap();
        assert_eq!(restored, plain);
    }

    #[test]
    fn compresses_repetitive_input() {
        let plain = sample_json();
        let compressed = compress(&plain);
        assert!(
            compressed.len() < plain.len(),
            "repetitive JSON must shrink: {} -> {}",
            plain.len(),
            compressed.len()
        );
    }

    #[test]
    fn decompress_rejects_plaintext_json() {
        // A legacy uncompressed object: its first byte is `{`, never the gzip
        // magic, so decoding fails loudly instead of returning the raw bytes.
        let error = decompress(br#"{"schema":1,"results":[],"padding":"xxxxxxxxxx"}"#).unwrap_err();
        drop(error);
    }

    #[test]
    fn decompress_rejects_unsupported_method_and_flags() {
        // A well-formed magic but an unexpected compression method or header
        // flags must be rejected, and each cause must be named distinctly so the
        // most-specific match arm — not a later catch-all — reports it.
        let mut bad_method = compress(&sample_json());
        *bad_method.get_mut(2).expect("header byte present") = 0x07;
        let error = decompress(&bad_method).unwrap_err();
        assert!(
            error.to_string().contains("compression method"),
            "an unsupported method must be named as such: {error}"
        );

        let mut bad_flags = compress(&sample_json());
        *bad_flags.get_mut(3).expect("header byte present") = 0x08;
        let error = decompress(&bad_flags).unwrap_err();
        assert!(
            error.to_string().contains("flags"),
            "unsupported header flags must be named as such: {error}"
        );
    }

    #[test]
    fn decompress_rejects_a_short_body_with_valid_magic() {
        // A buffer that carries the gzip magic but is too short to hold a header
        // and a trailer must fail, not slice out of bounds: the body would end
        // before the header does.
        let mut stored = GZIP_HEADER[..4].to_vec();
        stored.extend_from_slice(&[0_u8; TRAILER_LEN]);
        assert_eq!(stored.len(), 12, "magic plus a trailer, but no full header");
        let error = decompress(&stored).unwrap_err();
        assert!(
            error.to_string().contains("too short"),
            "a header-less body must be reported as too short: {error}"
        );
    }

    #[test]
    fn decompress_rejects_truncated_stream() {
        let mut compressed = compress(&sample_json());
        compressed.truncate(compressed.len().div_ceil(2));
        let error = decompress(&compressed).unwrap_err();
        drop(error);
    }

    #[test]
    fn decompress_rejects_empty_input() {
        let error = decompress(b"").unwrap_err();
        drop(error);
    }

    #[test]
    fn decompress_rejects_corrupt_crc() {
        let mut compressed = compress(&sample_json());
        // Flip a bit in the CRC-32 word (first of the eight trailer bytes).
        let crc_index = compressed.len() - TRAILER_LEN;
        *compressed.get_mut(crc_index).expect("trailer byte present") ^= 0xff;
        decompress(&compressed).unwrap_err();
    }

    #[test]
    fn decompress_rejects_corrupt_length() {
        let mut compressed = compress(&sample_json());
        // Flip a bit in the ISIZE word (first of the final four bytes).
        let isize_index = compressed.len() - 4;
        *compressed
            .get_mut(isize_index)
            .expect("trailer byte present") ^= 0xff;
        decompress(&compressed).unwrap_err();
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "multi-megabyte round trips are far too slow under Miri"
    )]
    fn roundtrips_a_large_compressible_payload_growing_inflate_many_times() {
        // A highly compressible payload deflates to a tiny body, so inflating it
        // grows the output buffer many times over — each growth re-reads the
        // decoder's input cursor, so the cursor must advance correctly every pass.
        let plain = vec![b'A'; 2_000_000];
        let compressed = compress(&plain);
        assert!(
            compressed.len().saturating_mul(100) < plain.len(),
            "a run of one byte must compress drastically: {} -> {}",
            plain.len(),
            compressed.len()
        );
        assert_eq!(decompress(&compressed).unwrap(), plain);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "multi-megabyte round trips are far too slow under Miri"
    )]
    fn compress_expands_incompressible_data_forcing_a_deflate_regrow() {
        // Already-compressed bytes cannot be shrunk, so deflating them produces
        // *more* output than the up-front capacity guess — forcing the compress
        // loop to grow its buffer mid-stream — yet must still round-trip exactly.
        let incompressible = compress(&incompressible_of(1_000_000));
        let compressed = compress(&incompressible);
        assert!(
            compressed.len() > incompressible.len(),
            "re-compressing gzip output must expand it: {} -> {}",
            incompressible.len(),
            compressed.len()
        );
        assert_eq!(decompress(&compressed).unwrap(), incompressible);
    }
}
