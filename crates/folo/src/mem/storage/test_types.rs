// Copyright (c) Microsoft Corporation.

use negative_impl::negative_impl;

// A type that fits the "simple cloneable type" pattern required by with_initial_value().
#[derive(Clone)]
pub struct SendAndSyncAndCloneType {
    pub value: usize,
}

impl SendAndSyncAndCloneType {
    pub fn new(value: usize) -> Self {
        Self { value }
    }
}

// A single-threaded type that cannot be simply cloned to each thread.
pub struct SingleThreadedType {
    pub value: usize,
}

impl SingleThreadedType {
    pub fn new(value: usize) -> Self {
        Self { value }
    }
}

#[negative_impl]
impl !Send for SingleThreadedType {}
#[negative_impl]
impl !Sync for SingleThreadedType {}
