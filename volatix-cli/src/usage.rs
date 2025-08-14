pub fn help() {
    let sections = vec![
        (
            "Administration",
            vec![
                ("RECONNECT", "Disconnect and try to reconnect to the server"),
                ("HELP", "Display this usage information"),
            ],
        ),
        (
            "Basic Operations",
            vec![
                (
                    "SET <key: string> <value: any>",
                    "Set a single key-value pair",
                ),
                ("GET <key>", "Get the value for a key"),
                ("DELETE <key>", "Delete a key"),
                ("EXISTS <key>", "Check if key exists"),
                ("FLUSH", "Clear the database"),
                ("KEYS", "Get a list of all key entries"),
                ("INCR <key>", "Increment an Int value by 1"),
                ("DECR <key>", "Decrement an Int value by 1"),
                (
                    "RENAME <old_key> <new_key>",
                    "Rename key retaining the entry",
                ),
            ],
        ),
        (
            "Batch Operations",
            vec![
                ("SETLIST key [value, value, ...]", "Set an array of values"),
                (
                    "SETMAP {\"key\": \"value\"}",
                    "Set a map of key-value pairs",
                ),
                ("GETLIST [key, key, ...]", "Get values for multiple keys"),
                ("DELETELIST [key, key, ...]", "Delete multiple keys"),
            ],
        ),
        (
            "Configuration",
            vec![
                ("CONFOPTIONS", "List configurable options"),
                ("  MAXCAP <u64>", "Max entries in DB"),
                ("  GLOBALTTL <u64>", "Default TTL for entries"),
                (
                    "  COMPRESSION <enable|disable>",
                    "Enable/disable compression",
                ),
                ("  COMPTHRESHOLD <u64>", "Size threshold for compression"),
                ("  EVICTPOLICY", "Eviction policy:"),
                ("    RFU", "Rarely frequently used"),
                ("    LFA", "Least frequently accessed"),
                ("    OLDEST", "Oldest entry first"),
                ("    SIZEAWARE", "Evict largest first"),
                ("CONFSET <key> <value>", "Set a config value"),
                ("CONFGET <key>", "Get a config value"),
            ],
        ),
        (
            "Stats",
            vec![
                ("GETSTATS", "Get global stats"),
                ("RESETSTATS", "Reset global stats"),
                ("DUMP <key>", "Get stats for a specific entry"),
            ],
        ),
        (
            "TTL (Time-to-Live)",
            vec![
                ("SETWTTL <key> <ttl: u64>", "Set key with TTL (in seconds)"),
                ("EXPIRE <key> <delta: i64>", "Extend or reduce TTL"),
                ("GETTTL <key>", "Get remaining TTL for key"),
                ("EVICTNOW <key>", "Trigger eviction using current policy"),
            ],
        ),
    ];

    println!("USAGE:\r\n");

    for (section, commands) in sections {
        println!("  {section}:\r");
        for (cmd, desc) in commands {
            println!("    {:<40} # {}\r", cmd, desc);
        }
        println!("\r");
    }
}
