use std::ffi::OsStr;
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Instant;

use rustix::fs::AtFlags;

type ResultSender = z_queue::defaults::BoundedSender<Result<PathBuf, rustix::io::Errno>>;
pub type ResultReceiver = z_queue::defaults::BoundedReceiver<Result<PathBuf, rustix::io::Errno>>;

struct Command {
    path: PathBuf,
    result_tx: ResultSender,
    deadline: Option<Instant>,
}

type CommandSender = z_queue::defaults::UnboundedSender<Command>;

static SENDER: LazyLock<CommandSender> = LazyLock::new(|| {
    let (tx, rx) = z_queue::unbounded();
    for _ in 0..4 {
        let rx = rx.clone();
        std::thread::spawn(move || {
            while let Ok(Command { path, result_tx, deadline }) = rx.recv() {
                if let Err(error) = walk(path, &result_tx, deadline) {
                    _ = result_tx.send(Err(error));
                }
            }
        });
    }
    tx
});

const BOUND: NonZeroUsize = NonZeroUsize::new(100).unwrap();

pub fn walkdir(path: PathBuf, deadline: Option<Instant>) -> ResultReceiver {
    let (tx, rx) = z_queue::bounded(BOUND);
    let command = Command { path, result_tx: tx, deadline };
    _ = SENDER.send(command);
    rx
}

pub async fn walkdir_async(path: PathBuf, deadline: Option<Instant>) -> ResultReceiver {
    let (tx, rx) = z_queue::bounded(BOUND);
    let command = Command { path, result_tx: tx, deadline };
    _ = SENDER.send_async(command).await;
    rx
}

fn walk(
    path: PathBuf,
    result_tx: &ResultSender,
    deadline: Option<Instant>,
) -> Result<(), rustix::io::Errno> {
    let file_type = {
        let stat = rustix::fs::lstat(&path)?;
        rustix::fs::FileType::from_raw_mode(stat.st_mode)
    };
    if !file_type.is_dir() {
        _ = result_tx.send(Ok(path));
        return Ok(());
    }

    if let Some(deadline) = deadline
        && Instant::now() > deadline
    {
        return Ok(());
    }

    read_dir(path, &result_tx, deadline)?;
    Ok(())
}

fn read_dir(
    mut path: PathBuf,
    result_tx: &ResultSender,
    deadline: Option<Instant>,
) -> Result<(), rustix::io::Errno> {
    let dir_fd = rustix::fs::open(
        &path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;

    if let Some(deadline) = deadline
        && Instant::now() > deadline
    {
        return Ok(());
    }

    let mut iter = rustix::fs::Dir::new(dir_fd)?;

    // Iterate over the entries.
    while let Some(entry_result) = iter.next() {
        if let Some(deadline) = deadline
            && Instant::now() > deadline
        {
            return Ok(());
        }

        let entry = entry_result?;

        let name_bytes = entry.file_name().to_bytes();
        if matches!(name_bytes, b"." | b"..") {
            continue;
        }
        let file_name = OsStr::from_bytes(name_bytes);
        let mut file_type = entry.file_type();

        if file_type == rustix::fs::FileType::Unknown {
            let dir_fd = iter.fd().unwrap();
            let stat = rustix::fs::statat(dir_fd, entry.file_name(), AtFlags::SYMLINK_NOFOLLOW)?;
            file_type = rustix::fs::FileType::from_raw_mode(stat.st_mode);
        }

        path.push(file_name);

        if file_type.is_dir() {
            let command = Command { path: path.clone(), result_tx: result_tx.clone(), deadline };
            _ = SENDER.send(command);
        }
        if result_tx.send(Ok(path.clone())).is_err() {
            break;
        }
        path.pop();
    }

    Ok(())
}
