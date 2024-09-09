use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use windows::{
    core::Owned,
    Win32::{
        Foundation::{HANDLE, INVALID_HANDLE_VALUE},
        System::IO::{
            CreateIoCompletionPort, GetQueuedCompletionStatusEx, PostQueuedCompletionStatus,
            OVERLAPPED_ENTRY,
        },
    },
};

criterion_group!(benches, win32_io);
criterion_main!(benches);

fn win32_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("win32_io");

    group.bench_function("post_queued_completion_status", |b| {
        b.iter_batched_ref(
            || unsafe {
                Owned::new(
                    CreateIoCompletionPort(INVALID_HANDLE_VALUE, HANDLE::default(), 0, 1).unwrap(),
                )
            },
            |completion_port| unsafe {
                PostQueuedCompletionStatus(**completion_port, 0, 0, None).unwrap()
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("get_queued_completion_status_ex_empty", |b| {
        b.iter_batched_ref(
            || unsafe {
                Owned::new(
                    CreateIoCompletionPort(INVALID_HANDLE_VALUE, HANDLE::default(), 0, 1).unwrap(),
                )
            },
            |completion_port| unsafe {
                let mut completed = vec![OVERLAPPED_ENTRY::default(); 1024];
                let mut completed_items: u32 = 0;

                GetQueuedCompletionStatusEx(
                    **completion_port,
                    completed.as_mut_slice(),
                    &mut completed_items as *mut _,
                    0,
                    false,
                )
                .unwrap_err(); // Expecting timeout error.
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("get_queued_completion_status_ex_partial", |b| {
        b.iter_batched_ref(
            || unsafe {
                let completion_port = Owned::new(
                    CreateIoCompletionPort(INVALID_HANDLE_VALUE, HANDLE::default(), 0, 1).unwrap(),
                );

                for _ in 0..512 {
                    PostQueuedCompletionStatus(*completion_port, 0, 0, None).unwrap()
                }

                completion_port
            },
            |completion_port| unsafe {
                let mut completed = vec![OVERLAPPED_ENTRY::default(); 1024];
                let mut completed_items: u32 = 0;

                GetQueuedCompletionStatusEx(
                    **completion_port,
                    completed.as_mut_slice(),
                    &mut completed_items as *mut _,
                    0,
                    false,
                )
                .unwrap();

                assert_eq!(completed_items, 512);
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("get_queued_completion_status_ex_overfull", |b| {
        b.iter_batched_ref(
            || unsafe {
                let completion_port = Owned::new(
                    CreateIoCompletionPort(INVALID_HANDLE_VALUE, HANDLE::default(), 0, 1).unwrap(),
                );

                for _ in 0..2000 {
                    PostQueuedCompletionStatus(*completion_port, 0, 0, None).unwrap()
                }

                completion_port
            },
            |completion_port| unsafe {
                let mut completed = vec![OVERLAPPED_ENTRY::default(); 1024];
                let mut completed_items: u32 = 0;

                GetQueuedCompletionStatusEx(
                    **completion_port,
                    completed.as_mut_slice(),
                    &mut completed_items as *mut _,
                    0,
                    false,
                )
                .unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}
