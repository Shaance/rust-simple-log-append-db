# Rust simple append only log based db
Simple db using append only log file. This runs in a single thread and is not suitable for any production environment. Implemented for learning purpose.

## How does this works?

Implemented methods:
- get `<key>`
- set `<key>` `<value>`
- delete `<key>`

A log file will be used to append values while an in-memory hashmap will keep track of the pair:
- key
- IndexValue object which has the 2 following properties:
  - offset: where to read the value in the file so we don't need to read the whole file
  - bytes_to_read: how many bytes to read from offset

When log file exceeds a certain size (1 MB in the `main.rs` file usage of the db), compaction process will kick-in. Compaction will remove any duplicate values in the log file, we can have many of those when updating multiple times a key's associated value.

Technically this could be thread-safe if we put a mutex on the set method (which writes to the log file AND runs compaction when needed).

We could also produce multiple log files that each have its own hashmap, read-only ones and 1 active where writes still happen. Compaction would then work accross all logs files.