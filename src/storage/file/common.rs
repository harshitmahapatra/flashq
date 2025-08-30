use std::{
    fs::{File, OpenOptions},
    io::Read,
    path::Path,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    None,
    Immediate,
    Periodic,
}

// ================================================================================================
// FILE I/O UTILITIES
// ================================================================================================

pub fn ensure_directory_exists<P: AsRef<Path>>(dir: P) -> Result<(), std::io::Error> {
    let dir = dir.as_ref();
    if !dir.exists() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}

pub fn open_file_for_append(file_path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(file_path)
}

pub fn sync_file_if_needed(file: &File, sync_mode: SyncMode) -> Result<(), std::io::Error> {
    if sync_mode == SyncMode::Immediate {
        file.sync_all()
    } else {
        Ok(())
    }
}

pub fn read_file_contents(file_path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let mut file = File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(buffer)
}
