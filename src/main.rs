use rand::Rng;
use simple_db::db::{Database, SimpleDB};
use simple_db::fs::LogFSHelper;

fn random_string(list: &Vec<&str>) -> String {
    let mut rng = rand::thread_rng();
    let random_index = rng.gen_range(0..list.len());
    list[random_index].to_string()
}

fn some_random_usage_of_db() {
    const ONE_MEGABYTE: usize = 1024 * 1024;
    let fs_helper = LogFSHelper::new(None, Some(ONE_MEGABYTE));
    let mut db = SimpleDB::new(Box::new(fs_helper));

    let keys: Vec<&str> = vec![
        "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10",
    ];

    let values: Vec<&str> = vec![
        "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9",
        "value10",
    ];

    const TOTAL_ITERATIONS: usize = 1_000_000;
    let mut set_count = 0;
    let mut delete_count = 0;

    for i in 0..TOTAL_ITERATIONS {
        db.get(&random_string(&keys));

        if i != 0 && i % 50_000 == 0 {
            println!(
                "Set {} random key value pairs in {} iterations",
                set_count, i
            );
        }
        if i % 2 == 0 {
            db.set(&random_string(&keys), &random_string(&values));
            set_count += 1;
        }
        if i % 10 == 0 {
            db.delete(&random_string(&keys));
            delete_count += 1;
        }
    }
    println!();
    println!("Total set count: {}", set_count);
    println!("Total delete count: {}", delete_count);
    println!("Total get count: {}", TOTAL_ITERATIONS);
}

fn main() {
    some_random_usage_of_db()
}
