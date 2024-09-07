use std::{
    error::Error,
    path::{Path, PathBuf},
};
use tracing::{event, Level};

const SCAN_PATH: &str = "c:\\Autodesk";

#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    let files = generate_file_list(SCAN_PATH);

    event!(Level::INFO, message = "scanning files", count = files.len());

    for _ in 0..25 {
        let tasks = files
            .iter()
            .cloned()
            .map(|file| {
                folo::rt::spawn_on_any(|| async {
                    _ = folo::fs::read(file).await;
                })
            })
            .collect::<Vec<_>>();

        for task in tasks {
            task.await;
        }
    }

    Ok(())
}

/// Generate a list of all files in target and all subdirectories recursively,
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
