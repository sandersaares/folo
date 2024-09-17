use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use folo::criterion::{ComparativeAdapter, FoloAdapter};
use std::{
    cell::LazyCell,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

criterion_group!(benches, file_io, scan_many_files);
criterion_main!(benches);

const FILE_SIZE: usize = 10 * 1024 * 1024 * 1024;
const FILE_PATH: &str = "testdata.bin";

const SMALL_FILE_SIZE: usize = 1024 * 1024 * 1024;
const SMALL_FILE_COUNT: usize = 32;
const SMALL_FILE_PATH: &str = "testdata_small.bin";

fn file_io(c: &mut Criterion) {
    let comparison_adapter =
        ComparativeAdapter::new(|| tokio::runtime::Builder::new_multi_thread().build().unwrap());

    // Create our test data files.
    File::create(FILE_PATH)
        .unwrap()
        .set_len(FILE_SIZE as u64)
        .unwrap();

    File::create(SMALL_FILE_PATH)
        .unwrap()
        .set_len(SMALL_FILE_SIZE as u64)
        .unwrap();

    {
        // Read the files once to ensure all benchmarks start with the files in OS cache.
        let mut temp = Vec::new();
        let mut f = File::open(FILE_PATH).unwrap();
        File::read_to_end(&mut f, &mut temp).unwrap();
        let mut f = File::open(SMALL_FILE_PATH).unwrap();
        temp.clear();
        File::read_to_end(&mut f, &mut temp).unwrap();
    }

    let mut group = c.benchmark_group("file_io");

    // This can take an eternity, so only repeat a few times.
    group.sample_size(10);

    group.bench_function("folo_read_file_to_vec", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_folo(Box::new(|| {
                    Box::pin(async move {
                        folo::rt::spawn_on_any(|| async {
                            let file = folo::fs::read(FILE_PATH).await.unwrap();
                            assert_eq!(file.len(), FILE_SIZE);
                        })
                        .await;
                    })
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("tokio_read_file_to_vec", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_competitor(Box::pin(async move {
                    _ = tokio::task::spawn(async {
                        let file = tokio::fs::read(FILE_PATH).await.unwrap();
                        assert_eq!(file.len(), FILE_SIZE);
                    })
                    .await;
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("folo_read_file_to_vec_many", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_folo(Box::new(|| {
                    Box::pin(async move {
                        let tasks = (0..SMALL_FILE_COUNT)
                            .map(|_| {
                                folo::rt::spawn_on_any(|| async {
                                    let file = folo::fs::read(SMALL_FILE_PATH).await.unwrap();
                                    assert_eq!(file.len(), SMALL_FILE_SIZE);
                                })
                            })
                            .collect::<Vec<_>>();

                        for task in tasks {
                            _ = task.await;
                        }
                    })
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("tokio_read_file_to_vec_many", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_competitor(Box::pin(async move {
                    let tasks = (0..SMALL_FILE_COUNT)
                        .map(|_| {
                            tokio::task::spawn(async {
                                let file = tokio::fs::read(SMALL_FILE_PATH).await.unwrap();
                                assert_eq!(file.len(), SMALL_FILE_SIZE);
                            })
                        })
                        .collect::<Vec<_>>();

                    for task in tasks {
                        _ = task.await;
                    }
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.finish();

    // Delete our test data files.
    std::fs::remove_file(FILE_PATH).unwrap();
    std::fs::remove_file(SMALL_FILE_PATH).unwrap();
}

const SCAN_PATH: &str = "c:\\Source";

// We read in every file in the target directory, recursively, concurrently.
fn scan_many_files(c: &mut Criterion) {
    let file_list = LazyCell::new(|| {
        // First make the list of files in advance - we want a vec of all the files in SCAN_PATH, recursive.
        let files = generate_file_list(SCAN_PATH);
        println!("Found {} files to scan.", files.len());
        files
    });

    // We use multithreaded mode for each, distributing the files across the processors for maximum
    // system-wide stress. Presumably this will lead to all threads being heavily used and stress
    // the I/O capabilities of the entire system (if a sufficiently rich input directory is used).
    let tokio = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let mut group = c.benchmark_group("scan_many_files");

    // This can take an extra eternity, so only repeat a few times.
    group.sample_size(10);

    group.bench_function("folo_scan_many_files", |b| {
        _ = &*file_list;

        b.to_async(FoloAdapter::default()).iter_batched(
            || file_list.clone(),
            |files| {
                folo::rt::spawn_on_any(move || async move {
                    let tasks = files
                        .iter()
                        .cloned()
                        .map(|file| {
                            folo::rt::spawn_on_any(|| async {
                                let _ = folo::fs::read(file).await;
                            })
                        })
                        .collect::<Vec<_>>();

                    for task in tasks {
                        task.await;
                    }
                })
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.bench_function("tokio_scan_many_files", |b| {
        b.to_async(&tokio).iter_batched(
            || file_list.clone(),
            |files| {
                tokio::task::spawn(async move {
                    let tasks = files
                        .iter()
                        .cloned()
                        .map(|file| {
                            tokio::task::spawn(async {
                                let _ = tokio::fs::read(file).await;
                            })
                        })
                        .collect::<Vec<_>>();

                    for task in tasks {
                        _ = task.await;
                    }
                })
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// Generate a list of all files in SCAN_PATH and all subdirectories recursively,
/// returning a boxed slice with their absolute paths.
fn generate_file_list(path: impl AsRef<Path>) -> Box<[PathBuf]> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            let sub_files = generate_file_list(path);
            files.extend_from_slice(&sub_files);
        } else {
            files.push(path);
        }
    }

    files.into_boxed_slice()
}
