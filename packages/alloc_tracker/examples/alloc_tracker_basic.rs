//! Demonstrating key `alloc_tracker` types working together.
//!
//! Run with: `cargo run --example comprehensive_tracking`.

use std::collections::HashMap;
use std::hint::black_box;

use alloc_tracker::{Allocator, Session};

#[global_allocator]
static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();

fn main() {
    println!("=== Allocation Tracking Example ===");
    println!();

    // Create a tracking session - this enables allocation monitoring
    let session = Session::new();
    println!("✓ Created tracking session");
    println!();

    // Track string formatting - batch operation for efficiency
    {
        let string_op = session.operation("string_formatting");
        let _span = string_op.measure_thread().iterations(3);
        for i in 0..3 {
            let s = format!("String number {i} with some content");
            black_box(s);
        }
    }

    // Track hashmap creation - batch operation for efficiency
    {
        let hashmap_op = session.operation("hashmap_creation");
        let _span = hashmap_op.measure_thread().iterations(3);
        for _ in 0..3 {
            let mut map = HashMap::new();
            map.insert("key1", "value1");
            map.insert("key2", "value2");
            map.insert("key3", "value3");
            black_box(map);
        }
    }

    // Track vector allocation - batch operation for efficiency
    {
        let vector_op = session.operation("vector_allocation");
        let _span = vector_op.measure_thread().iterations(3);
        for i in 0..3 {
            let vec = vec![i; 50]; // 50 elements each time
            black_box(vec);
        }
    }

    // Dropping the session prints its results to stdout and writes the JSON
    // output files.
    drop(session);

    println!();
    println!("The session's results were printed above and written to");
    println!("target/alloc_tracker/ as machine-readable JSON.");
}
