pub fn help() {
    println!("USAGE:");
    println!();

    println!("  Basic Operations:");
    println!("    SET <key: string> <value: any>         # Set a single key-value pair");
    println!("    GET <key>                              # Get the value for a key");
    println!("    DELETE <key>                           # Delete a key");
    println!("    EXISTS <key>                           # Check if key exist");
    println!("    FLUSH                                  # Clear the database");
    println!("    KEYS                                   # Get a list of all key entries");
    println!("    INCR <key>                             # Increment an Int value by 1");
    println!("    DECR <key>                             # Decrement an Int value by 1");
    println!("    RENAME <old_key> <new_key>             # Rename key retaining the entry");
    println!();

    println!("  Batch Operations:");
    println!("    SETLIST key [value, value, ...]              # Set an array of values");
    println!("    SETMAP {{\"key\": \"value\"}}                # Set a map of key-value pairs");
    println!("    GETLIST [key, key, ...]                # Get values for multiple keys");
    println!("    DELETELIST [key, key, ...]             # Delete multiple keys");
    println!();

    println!("  Configuration:");
    println!("    CONFOPTIONS                            # List configurable options");
    println!("      MAXCAP <u64>                         # Max entries in DB");
    println!("      GLOBALTTL <u64>                      # Default TTL for entries");
    println!("      COMPRESSION <enable|disable>         # Enable/disable compression");
    println!("      COMPTHRESHOLD <u64>                  # Size threshold for compression");
    println!("      EVICTPOLICY                          # Eviction policy:");
    println!("        RFU                                # Rarely frequently used");
    println!("        LFA                                # Least frequently accessed");
    println!("        OLDEST                             # Oldest entry first");
    println!("        SIZEAWARE                          # Evict largest first");
    println!("    CONFSET <key> <value>                  # Set a config value");
    println!("    CONFGET <key>                          # Get a config value");
    println!();

    println!("  Stats:");
    println!("    GETSTATS                               # Get global stats");
    println!("    RESETSTATS                             # Reset global stats");
    println!("    DUMP <key>                             # Get stats for a specific entry");
    println!();

    println!("  TTL (Time-to-Live):");
    println!("    SETWTTL <key> <ttl: u64>               # Set key with TTL (in seconds)");
    println!("    EXPIRE <key> <delta: i64>              # Extend or reduce TTL");
    println!("    GETTTL <key>                           # Get remaining TTL for key");
    println!(
        "    EVICTNOW <key>                         # Trigger Eviction using current eviction policy"
    );
    println!();
}
