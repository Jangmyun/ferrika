//! Writing files that may hold secrets or record data: owner-only (0600)
//! on Unix.

use std::fs;
use std::path::Path;

/// Opens `path` for writing a record export. On unix the file is created
/// with mode 0600 (and an existing one narrowed to it), since exports hold
/// record data that umask-default 0644 would show to other local users.
/// `append` keeps existing content; otherwise the file is truncated.
pub fn open_private_file(path: &Path, append: bool) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create(true);
    if append {
        options.append(true);
    } else {
        options.write(true).truncate(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.mode(0o600);
        let file = options.open(path)?;
        // Only a regular file has a mode to narrow: `/dev/null`, `/dev/stdout`
        // or a FIFO belong to someone else and refuse chmod (EPERM).
        if file.metadata()?.is_file() {
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        options.open(path)
    }
}

/// Writes `contents` to `path` through a temp file in the same directory that
/// is renamed into place, so a crash never leaves a half-written file and
/// (on Unix) the file is owner-only (0600) from the start: it may hold
/// plaintext passwords.
pub fn write_private_file(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("path has no file name"))?;
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(format!(".tmp-{}", std::process::id()));
    let tmp = path.with_file_name(tmp_name);

    let result = (|| {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        // A leftover temp file keeps its old mode; tighten it explicitly.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

// Only Unix has file modes to check.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn private_file_is_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::current_dir()
            .unwrap()
            .join(format!("target/test-private-file-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("records.jsonl");
        let _ = std::fs::remove_file(&path);
        for append in [true, false] {
            let _ = std::fs::remove_file(&path);
            drop(open_private_file(&path, append).unwrap());
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "append={append}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn device_files_can_be_opened_for_export() {
        // `consume --save /dev/null` must not fail on chmod (EPERM).
        for append in [true, false] {
            open_private_file(Path::new("/dev/null"), append).unwrap();
        }
    }
}
