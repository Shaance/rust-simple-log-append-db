use crate::fs::FSHelper;

pub struct SimpleDB {
    index: std::collections::HashMap<String, IndexValue>,
    fs_helper: Box<dyn FSHelper>,
}

struct IndexValue {
    offset: u64,
    bytes_to_read: usize,
}

pub trait Database {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&mut self, key: &str, value: &str);
    fn delete(&mut self, key: &str);
}

impl Database for SimpleDB {
    fn get(&self, key: &str) -> Option<String> {
        if let None = self.index.get(key) {
            return None;
        }
        let index_value = &self.index.get(key).unwrap();
        let value = self
            .fs_helper
            .read_from_log_file(index_value.offset, index_value.bytes_to_read);
        Some(value)
    }

    fn set(&mut self, key: &str, value: &str) {
        // this is not thread-safe but this program is not multi-threaded anyway
        let offset = self.fs_helper.get_log_file_size();
        let written_bytes = self
            .fs_helper
            .write_to_log_file(&value, &self.fs_helper.get_log_file_path());
        self.index.insert(
            String::from(key),
            IndexValue {
                offset,
                bytes_to_read: written_bytes,
            },
        );
        if (offset + written_bytes as u64) > self.fs_helper.get_max_bytes_per_file() as u64 {
            self.run_compaction();
        }
    }

    fn delete(&mut self, key: &str) {
        self.index.remove(key);
    }
}

impl SimpleDB {
    pub fn new(fs_helper: Box<dyn FSHelper>) -> Self {
        let index = std::collections::HashMap::new();
        fs_helper.create_file_if_not_exists(&fs_helper.get_log_file_path());
        Self { index, fs_helper }
    }

    fn run_compaction(&mut self) {
        // this is not thread-safe but this program is not multi-threaded anyway
        println!(
            "Log file size before compaction: {} bytes",
            self.fs_helper.get_log_file_size()
        );
        let mut new_index: std::collections::HashMap<String, IndexValue> =
            std::collections::HashMap::new();
        const TMP_NEW_LOG_FILE_PATH: &str = "log_tmp";

        self.fs_helper
            .create_file_if_not_exists(TMP_NEW_LOG_FILE_PATH);

        let mut total_written_bytes = 0;
        for (key, index_value) in &self.index {
            let value = self
                .fs_helper
                .read_from_log_file(index_value.offset, index_value.bytes_to_read);
            let written_bytes = self
                .fs_helper
                .write_to_log_file(&value, TMP_NEW_LOG_FILE_PATH);
            new_index.insert(
                key.to_string(),
                IndexValue {
                    offset: total_written_bytes,
                    bytes_to_read: written_bytes,
                },
            );
            total_written_bytes += written_bytes as u64;
        }

        const OLD_LOG_FILE_PATH: &str = "log_old";
        self.fs_helper
            .rename_file(&self.fs_helper.get_log_file_path(), OLD_LOG_FILE_PATH);
        self.fs_helper
            .rename_file(TMP_NEW_LOG_FILE_PATH, &self.fs_helper.get_log_file_path());

        self.index = new_index;

        self.fs_helper.delete_file(OLD_LOG_FILE_PATH);
        self.fs_helper.delete_file(TMP_NEW_LOG_FILE_PATH);
        println!(
            "Log file size after compaction: {} bytes",
            self.fs_helper.get_log_file_size()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{LogFSHelper, MockFSHelper};
    use mockall::predicate;

    #[test]
    fn test_set_get_delete() -> Result<(), Box<dyn std::error::Error>> {
        let file = assert_fs::NamedTempFile::new("log_test")?;
        let fs_helper = LogFSHelper::new(file.path().to_str(), None);
        let mut db = SimpleDB::new(Box::new(fs_helper));

        db.set("key", "value");
        assert_eq!(db.get("key"), Some("value".to_string()));
        assert_eq!(db.get("key2"), None);

        db.set("key", "value2");
        assert_eq!(db.get("key"), Some("value2".to_string()));

        db.delete("key");
        assert_eq!(db.get("key"), None);

        Ok(())
    }

    #[test]
    fn test_fs_interaction() {
        let mut fs_mock = MockFSHelper::new();
        fs_mock
            .expect_get_log_file_path()
            .return_const(String::from("log"));
        fs_mock
            .expect_create_file_if_not_exists()
            .times(1)
            .with(predicate::eq("log"))
            .return_const(());
        fs_mock
            .expect_get_log_file_size()
            .times(1)
            .return_const(0u32)
            .times(1)
            .return_const(100u32)
            .times(1)
            .return_const(0u32);
        fs_mock
            .expect_get_max_bytes_per_file()
            .times(1)
            .return_const(100usize);
        fs_mock
            .expect_write_to_log_file()
            .times(1)
            .with(predicate::eq("value"), predicate::eq("log"))
            .return_const(5usize);
        fs_mock
            .expect_read_from_log_file()
            .times(1)
            .with(predicate::eq(0), predicate::eq(5))
            .return_const("value".to_string());
        fs_mock.expect_rename_file().times(0);
        fs_mock.expect_delete_file().times(0);

        let mut db = SimpleDB::new(Box::new(fs_mock));
        db.set("key", "value");
        db.get("key");
        db.delete("key");
    }
}
