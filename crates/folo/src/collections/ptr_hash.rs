use std::hash::{BuildHasherDefault, Hasher};

/// Pointers tend to look very similar near the start of the value, with only the end differing.
///
/// ```text
/// 0x0000000012341234
/// 0x0000000056785678
/// 0x0001C00B59595959
/// 0x0001C00B59636363
/// ```
///
/// This can lead to unbalanced hash tables, as the first few bits are the same for all pointers.
/// We do not necessarily know which bits are the most significant, so we xor the pointer with its
/// own bit-reversed value to help spread around the entropy.
#[derive(Debug, Default)]
pub struct PointerHasher {
    value: u64,
}

impl Hasher for PointerHasher {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, bytes: &[u8]) {
        assert_eq!(
            bytes.len(),
            8,
            "PointerHasher only supports 64-bit pointers."
        );

        let input_raw = bytes.as_ptr() as *const u64;

        // SAFETY: We verified that the input is 8 bytes long, so we can safely treat it as u64.
        unsafe {
            self.value ^= *input_raw;
            self.value ^= (*input_raw).reverse_bits();
        }
    }
}

pub type BuildPointerHasher = BuildHasherDefault<PointerHasher>;
