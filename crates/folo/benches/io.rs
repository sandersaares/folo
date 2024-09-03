use criterion::{async_executor::AsyncExecutor, criterion_group, criterion_main, Criterion};

criterion_group!(benches, file_io);
criterion_main!(benches);

const FILE_SIZE: usize = 100 * 1024 * 1024;
const FILE_PATH: &str = "testdata.bin";

fn file_io(c: &mut Criterion) {
    let tokio = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let tokio_local = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // Create a large testdata.bin file, overwriting if it exists.
    let testdata = std::iter::repeat(0u8).take(FILE_SIZE).collect::<Vec<_>>();
    std::fs::write(FILE_PATH, &testdata).unwrap();

    let mut group = c.benchmark_group("file_io");

    group.bench_function("folo_read_file_to_vec", |b| {
        b.to_async(FoloExecutor).iter(|| {
            folo::rt::spawn_on_any(|| async {
                folo::fs::read(FILE_PATH).await.unwrap();
            })
        });
    });

    group.bench_function("tokio_read_file_to_vec", |b| {
        b.to_async(&tokio_local).iter(|| {
            tokio::task::spawn(async {
                tokio::fs::read(FILE_PATH).await.unwrap();
            })
        });
    });

    group.finish();

    // Delete our test data file.
    std::fs::remove_file(FILE_PATH).unwrap();
}

// TODO: Make this reusable.
/// We use a two-fold strategy:
///
/// * For the most part, the executor works "normally" during the benchmark.
/// * However, the entrypoint from Criterion is a `block_on` which is a bit of a common antipattern
///   in Rust that it is assumed the executor wants to execute stuff on the current thread. This is
///   not the case with Folo - we deliberately use worker threads *only*. So we use the `futures`
///   lightweight local executor to bootstrap the whole process, using it to poll the first future.
///   This is only legal because we immediately `spawn_on_any()` to bring the work to a Folo worker.
struct FoloExecutor;

impl AsyncExecutor for FoloExecutor {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        _ = folo::rt::ExecutorBuilder::new()
            // This allows `spawn_on_any()` to work correctly and shovel work to Folo.
            // It also causes the executor to be reused - it is only created on the first build.
            .ad_hoc_entrypoint()
            .build()
            .unwrap();

        // Note that we leave the Folo executor running, so it can be reused.
        futures::executor::block_on(future)
    }
}
