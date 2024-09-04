use criterion::{criterion_group, criterion_main, Criterion};
use folo::criterion::FoloAdapter;

criterion_group!(benches, file_io);
criterion_main!(benches);

const FILE_SIZE: usize = 100 * 1024 * 1024;
const FILE_PATH: &str = "testdata.bin";

fn file_io(c: &mut Criterion) {
    let tokio_local = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // Create a large testdata.bin file, overwriting if it exists.
    let testdata = std::iter::repeat(0u8).take(FILE_SIZE).collect::<Vec<_>>();
    std::fs::write(FILE_PATH, &testdata).unwrap();

    let mut group = c.benchmark_group("file_io");

    group.bench_function("folo_read_file_to_vec", |b| {
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
                let file = folo::fs::read(FILE_PATH).await.unwrap();
                assert_eq!(file.len(), FILE_SIZE);
            })
        });
    });

    group.bench_function("tokio_read_file_to_vec", |b| {
        b.to_async(&tokio_local).iter(|| {
            tokio::task::spawn(async {
                let file = tokio::fs::read(FILE_PATH).await.unwrap();
                assert_eq!(file.len(), FILE_SIZE);
            })
        });
    });

    group.finish();

    // Delete our test data file.
    std::fs::remove_file(FILE_PATH).unwrap();
}
