use mockall::automock;
use std::{
    env, fs,
    io::{Read, Seek, SeekFrom, Write},
};

#[automock]
pub trait FSHelper {
    fn get_log_file_path(&self) -> &str;
    fn get_log_file_size(&self) -> u64;
    fn get_max_bytes_per_file(&self) -> usize;
    fn create_file_if_not_exists(&self, path: &str);
    fn write_to_log_file(&self, value: &str, path: &str) -> usize;
    fn read_from_log_file(&self, offset: u64, bytes_to_read: usize) -> String;
    fn delete_file(&self, path: &str);
    fn rename_file(&self, old_path: &str, new_path: &str);
}

pub struct LogFSHelper {
    log_file_path: String,
    max_bytes_per_file: usize,
}

impl FSHelper for LogFSHelper {
    fn get_log_file_path(&self) -> &str {
        &self.log_file_path
    }

    fn get_log_file_size(&self) -> u64 {
        self.get_file_size(&self.log_file_path)
    }

    fn get_max_bytes_per_file(&self) -> usize {
        self.max_bytes_per_file
    }

    fn create_file_if_not_exists(&self, file_path: &str) {
        if !std::path::Path::new(file_path).exists() {
            fs::File::create(file_path).expect(format!("Failed to create file {}", file_path).as_str());
        }
    }

    fn write_to_log_file(&self, data: &str, log_file_path: &str) -> usize {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(log_file_path)
            .expect(format!("Failed to open file {}", log_file_path).as_str());
        let bytes = data.as_bytes();
        file.write_all(bytes).expect(format!("Failed to write to file {}", log_file_path).as_str());
        bytes.len()
    }

    fn read_from_log_file(&self, position: u64, bytes_to_read: usize) -> String {
        self.read_from_file(&self.log_file_path, position, bytes_to_read)
    }

    fn delete_file(&self, file_path: &str) {
        if std::path::Path::new(file_path).exists() {
            fs::remove_file(file_path).expect(format!("Unable to delete file {}", file_path).as_str());
        }
    }

    fn rename_file(&self, old_file_path: &str, new_file_path: &str) {
        if std::path::Path::new(old_file_path).exists() {
            fs::rename(old_file_path, new_file_path).expect(format!("Unable to rename file {} to {}", old_file_path, new_file_path).as_str());
        }
    }
}

impl LogFSHelper {
    const MAX_BYTES_PER_FILE: usize = 1024 * 1024 * 10; // 10 MB

    pub fn new(log_file_path: Option<&str>, max_bytes_per_file: Option<usize>) -> Self {
        let default_path = format!("{}/{}", env::current_dir().unwrap().display(), "log");
        // if max_bytes_per_file is provided, check if it's positive, otherwise panic
        let max_bytes_per_file = max_bytes_per_file.unwrap_or(Self::MAX_BYTES_PER_FILE);
        assert!(
            max_bytes_per_file > 0,
            "max_bytes_per_file must be strictly positive"
        );

        Self {
            log_file_path: log_file_path.unwrap_or(&default_path).to_string(),
            max_bytes_per_file,
        }
    }

    fn read_from_file(&self, file_path: &str, position: u64, bytes_to_read: usize) -> String {
        let mut file = fs::File::open(file_path).expect(format!("Unable to open file {}", file_path).as_str());
        file.seek(SeekFrom::Start(position))
            .expect(format!("Unable to seek to position {}", position).as_str());
        let mut buffer = vec![0u8; bytes_to_read];
        file.read_exact(&mut buffer).expect(format!("Unable to read {} bytes from file {}", bytes_to_read, file_path).as_str());
        String::from_utf8_lossy(&buffer).to_string()
    }

    fn get_file_size(&self, file_path: &str) -> u64 {
        let metadata = fs::metadata(file_path).expect(format!("Unable to get metadata for file {}", file_path).as_str());
        metadata.len()
    }
}
