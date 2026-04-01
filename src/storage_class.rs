use std::ffi::CStr;
use std::io::Write;
use std::path::{Path, PathBuf};

use compio::BufResult;
use compio::io::{AsyncReadAt, AsyncReadAtExt};
use rustix::fs::{Mode, OFlags};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum StorageClass {
    Hdd { major: u32, minor: u32 },
    SsdOrUnknown,
}

impl StorageClass {
    pub fn determine(path: &Path) -> Result<Self, rustix::io::Errno> {
        let stat = rustix::fs::stat(path)?;
        Self::from_stat(&stat)
    }

    pub async fn determine_async(path: &Path) -> Result<Self, std::io::Error> {
        let metadata = compio::fs::metadata(path).await?;
        Self::from_compio_metadata(&metadata).await
    }

    pub fn from_stat(stat: &rustix::fs::Stat) -> Result<Self, rustix::io::Errno> {
        let dev = stat.st_dev;

        let major_num = rustix::fs::major(dev);
        let minor_num = rustix::fs::minor(dev);

        // A u32 is at most 10 digits long.
        // "/sys/dev/block/" (15) + max u32 (10) + ":" (1) + max u32 (10) + '\0' (1) = 37 bytes
        // maximum.
        let mut buf = [0u8; 37];

        write!(&mut buf[..], "/sys/dev/block/{major_num}:{minor_num}\0")
            .expect("Buffer too small for sysfs path");

        let sysfs_link = CStr::from_bytes_until_nul(&buf).expect("Failed to find null terminator");

        let mut current_dir_fd = match rustix::fs::open(
            sysfs_link,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(_) => return Ok(StorageClass::SsdOrUnknown),
        };

        loop {
            // Check for rotational flag
            if let Ok(rot_fd) = rustix::fs::openat(
                &current_dir_fd,
                "queue/rotational",
                OFlags::RDONLY | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                let mut buf = [0u8; 1];

                if let Ok(1) = rustix::io::read(&rot_fd, &mut buf) {
                    if buf[0] == b'1' {
                        // We found the physical HDD directory!
                        // Now, read the "dev" file in this directory to get the root major:minor.
                        if let Ok(dev_fd) = rustix::fs::openat(
                            &current_dir_fd,
                            "dev",
                            OFlags::RDONLY | OFlags::CLOEXEC,
                            Mode::empty(),
                        ) {
                            let mut dev_buf = [0u8; 16];
                            if let Ok(bytes_read) = rustix::io::read(&dev_fd, &mut dev_buf) {
                                // Parse "8:0\n" into u32 integers
                                if let Ok(dev_str) = std::str::from_utf8(&dev_buf[..bytes_read]) {
                                    if let Some((maj_str, min_str)) = dev_str.trim().split_once(':')
                                    {
                                        if let (Ok(major), Ok(minor)) =
                                            (maj_str.parse(), min_str.parse())
                                        {
                                            return Ok(Self::Hdd { major, minor });
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        return Ok(Self::SsdOrUnknown);
                    }
                }
            }

            // Walk up the VFS tree
            let parent_dir_fd = match rustix::fs::openat(
                &current_dir_fd,
                "..",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(_) => break,
            };

            let current_stat = rustix::fs::fstat(&current_dir_fd)?;
            let parent_stat = rustix::fs::fstat(&parent_dir_fd)?;

            if current_stat.st_dev == parent_stat.st_dev
                && current_stat.st_ino == parent_stat.st_ino
            {
                break;
            }

            current_dir_fd = parent_dir_fd;
        }

        Ok(Self::SsdOrUnknown)
    }

    pub async fn from_compio_metadata(
        metadata: &compio::fs::Metadata,
    ) -> Result<Self, std::io::Error> {
        use std::os::unix::fs::MetadataExt;

        let dev = metadata.dev();
        let major_num = rustix::fs::major(dev);
        let minor_num = rustix::fs::minor(dev);

        // Construct the sysfs path directly
        let sysfs_path = format!("/sys/dev/block/{major_num}:{minor_num}");
        let mut current_dir = PathBuf::from(sysfs_path);

        loop {
            // Check for rotational flag using io_uring
            let rot_path = current_dir.join("queue/rotational");
            if let Ok(rot_file) = compio::fs::File::open(&rot_path).await {
                let buf = [0u8; 1];
                if let BufResult(Ok(()), buf) = rot_file.read_exact_at(buf, 0).await {
                    if buf[0] == b'1' {
                        let dev_path = current_dir.join("dev");
                        if let Ok(dev_file) = compio::fs::File::open(&dev_path).await
                            // Read up to 16 bytes. compio's read returns bytes read.
                            && let BufResult(Ok(bytes_read), dev_buf) =
                                dev_file.read_at([0u8; 16], 0).await
                            && let Ok(dev_str) = std::str::from_utf8(&dev_buf[..bytes_read])
                            && let Some((maj, min)) = dev_str.trim().split_once(':')
                            && let (Ok(major), Ok(minor)) = (maj.parse(), min.parse())
                        {
                            return Ok(Self::Hdd { major, minor });
                        }
                    } else {
                        return Ok(Self::SsdOrUnknown);
                    }
                }
            }

            // Walk up the VFS tree by popping the path
            let current_stat = match compio::fs::metadata(&current_dir).await {
                Ok(s) => s,
                Err(_) => break,
            };
            current_dir.pop();

            if current_dir.as_os_str().is_empty() {
                break; // Hit root
            }

            let parent_stat = match compio::fs::metadata(&current_dir).await {
                Ok(s) => s,
                Err(_) => break,
            };

            if current_stat.dev() == parent_stat.dev() && current_stat.ino() == parent_stat.ino() {
                break;
            }
        }

        Ok(Self::SsdOrUnknown)
    }
}
