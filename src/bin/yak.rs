use std::path::Path;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use yakvdb::api::Store;
use yakvdb::disk::block::Block;
use yakvdb::disk::file::File;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("Usage: yak <path>");
        std::process::exit(1);
    }
    let path = Path::new(&args[1]);
    if !path.exists() {
        eprintln!("File does not exist: {}", path.display());
        std::process::exit(1);
    }
    let Ok(file) = File::<Block>::open(path) else {
        eprintln!("Failed to open file: {}", path.display());
        std::process::exit(1);
    };

    println!("yak CLI started. Type 'help' for commands. Type 'exit' to quit.");

    // Helper to parse a single argument as bytes, supporting:
    // - 0x... hex
    // - "...", '...' quoted string
    // - plain string
    fn parse_arg_bytes(arg: &str) -> Option<Vec<u8>> {
        let arg = arg.trim();
        if arg.starts_with("0x") || arg.starts_with("0X") {
            hex::decode(&arg[2..]).ok()
        } else if (arg.starts_with('"') && arg.ends_with('"')) || (arg.starts_with('\'') && arg.ends_with('\'')) {
            Some(arg[1..arg.len()-1].as_bytes().to_vec())
        } else {
            Some(arg.as_bytes().to_vec())
        }
    }

    // Helper to parse a line into words, preserving quoted substrings as single arguments
    fn split_args(input: &str) -> Vec<String> {
        let mut args = Vec::new();
        let mut in_quotes = false;
        let mut quote_char = '\0';
        let mut current = String::new();
        for c in input.chars() {
            if in_quotes {
                if c == quote_char {
                    in_quotes = false;
                    current.push(c);
                    args.push(current.clone());
                    current.clear();
                } else {
                    current.push(c);
                }
            } else {
                if c == '"' || c == '\'' {
                    in_quotes = true;
                    quote_char = c;
                    current.push(c);
                } else if c.is_whitespace() {
                    if !current.is_empty() {
                        args.push(current.clone());
                        current.clear();
                    }
                } else {
                    current.push(c);
                }
            }
        }
        if !current.is_empty() {
            args.push(current);
        }
        args
    }

    enum Mode {
        Str,
        Hex,
    }
    let mut mode = Mode::Hex;
    
    // Initialize rustyline editor with history
    let mut rl = DefaultEditor::new().expect("Failed to initialize readline");
    
    // Try to load history from file (ignore errors if file doesn't exist)
    let history_path = format!("{}.history", path.display());
    let _ = rl.load_history(&history_path);
    
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                
                // Add command to history
                let _ = rl.add_history_entry(input);
                
                let args = split_args(input);
                if args.is_empty() {
                    continue;
                }
                let cmd = args[0].as_str();

                match cmd {
            "lookup" => {
                if args.len() < 2 {
                    println!("Usage: lookup <key>");
                    continue;
                }
                if let Some(key_bytes) = parse_arg_bytes(&args[1]) {
                    match file.lookup(&key_bytes) {
                        Ok(Some(val)) => {
                            match mode {
                                Mode::Hex => println!("Value: 0x{}", hex::encode(&val)),
                                Mode::Str => println!("Value: {}", String::from_utf8_lossy(&val)),
                            }
                        }
                        Ok(None) => {
                            println!("Key not found");
                        }
                        Err(e) => {
                            println!("Error: {:?}", e);
                        }
                    }
                } else {
                    println!("Invalid key format");
                }
            }
            "insert" => {
                if args.len() < 3 {
                    println!("Usage: insert <key> <value>");
                    continue;
                }
                let key_bytes = match parse_arg_bytes(&args[1]) {
                    Some(b) => b,
                    None => {
                        println!("Invalid key format");
                        continue;
                    }
                };
                let val_bytes = match parse_arg_bytes(&args[2]) {
                    Some(b) => b,
                    None => {
                        println!("Invalid value format");
                        continue;
                    }
                };
                match file.insert(&key_bytes, &val_bytes) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "remove" => {
                if args.len() < 2 {
                    println!("Usage: remove <key>");
                    continue;
                }
                if let Some(key_bytes) = parse_arg_bytes(&args[1]) {
                    match file.remove(&key_bytes) {
                        Ok(()) => println!("OK"),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Invalid key format");
                }
            }
            "min" => {
                match file.min() {
                    Ok(Some(key)) => {
                        match mode {
                            Mode::Hex => println!("Min key: 0x{}", hex::encode(&key)),
                            Mode::Str => println!("Min key: {}", String::from_utf8_lossy(&key)),
                        }
                    }
                    Ok(None) => println!("No minimum key (empty database)"),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "max" => {
                match file.max() {
                    Ok(Some(key)) => {
                        match mode {
                            Mode::Hex => println!("Max key: 0x{}", hex::encode(&key)),
                            Mode::Str => println!("Max key: {}", String::from_utf8_lossy(&key)),
                        }
                    }
                    Ok(None) => println!("No maximum key (empty database)"),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "len" | "size" | "count" => {
                match file.min() {
                    Ok(Some(mut current_key)) => {
                        let mut count = 1; // Count the min key
                        
                        // Iterate through all keys using above()
                        loop {
                            match file.above(&current_key) {
                                Ok(Some(next_key)) => {
                                    count += 1;
                                    current_key = next_key;
                                }
                                Ok(None) => break, // No more keys
                                Err(e) => {
                                    println!("Error during iteration: {:?}", e);
                                    return;
                                }
                            }
                        }
                        
                        println!("Total entries: {}", count);
                    }
                    Ok(None) => println!("Total entries: 0 (empty database)"),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "from" => {
                if args.len() < 3 {
                    println!("Usage: from <key> <number>");
                    continue;
                }
                if let Some(start_key) = parse_arg_bytes(&args[1]) {
                    if let Ok(number) = args[2].parse::<usize>() {
                        if number == 0 {
                            println!("Number must be greater than 0");
                            continue;
                        }
                        
                        // Start from the key above the given key (exclusive)
                        match file.above(&start_key) {
                            Ok(Some(mut current_key)) => {
                                // Print the first entry
                                if let Ok(Some(value)) = file.lookup(&current_key) {
                                    match mode {
                                        Mode::Hex => println!("{}: 0x{} -> 0x{}", 1, hex::encode(&current_key), hex::encode(&value)),
                                        Mode::Str => println!("{}: {} -> {}", 1, String::from_utf8_lossy(&current_key), String::from_utf8_lossy(&value)),
                                    }
                                    
                                    // Print remaining entries
                                    for i in 2..=number {
                                        match file.above(&current_key) {
                                            Ok(Some(next_key)) => {
                                                if let Ok(Some(value)) = file.lookup(&next_key) {
                                                    match mode {
                                                        Mode::Hex => println!("{}: 0x{} -> 0x{}", i, hex::encode(&next_key), hex::encode(&value)),
                                                        Mode::Str => println!("{}: {} -> {}", i, String::from_utf8_lossy(&next_key), String::from_utf8_lossy(&value)),
                                                    }
                                                    current_key = next_key;
                                                } else {
                                                    break;
                                                }
                                            }
                                            Ok(None) => break, // No more entries
                                            Err(e) => {
                                                println!("Error during iteration: {:?}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                println!("No entries found after the given key");
                            }
                            Err(e) => {
                                println!("Error: {:?}", e);
                            }
                        }
                    } else {
                        println!("Invalid number: {}", args[2]);
                    }
                } else {
                    println!("Invalid key format");
                }
            }
            "till" => {
                if args.len() < 3 {
                    println!("Usage: till <key> <number>");
                    continue;
                }
                if let Some(end_key) = parse_arg_bytes(&args[1]) {
                    if let Ok(number) = args[2].parse::<usize>() {
                        if number == 0 {
                            println!("Number must be greater than 0");
                            continue;
                        }
                        
                        // Collect all keys up to but not including the end_key (exclusive)
                        let mut entries = Vec::new();
                        
                        match file.min() {
                            Ok(Some(mut current_key)) => {
                                // Check if min key is < end_key (exclusive)
                                if current_key < end_key {
                                    if let Ok(Some(value)) = file.lookup(&current_key) {
                                        entries.push((current_key.clone(), value));
                                    }
                                    
                                    // Continue until we reach the end_key (but don't include it)
                                    loop {
                                        match file.above(&current_key) {
                                            Ok(Some(next_key)) => {
                                                if next_key < end_key {
                                                    if let Ok(Some(value)) = file.lookup(&next_key) {
                                                        entries.push((next_key.clone(), value));
                                                    }
                                                    current_key = next_key;
                                                } else {
                                                    break; // Stop when we reach or pass the end_key
                                                }
                                            }
                                            Ok(None) => break,
                                            Err(e) => {
                                                println!("Error during iteration: {:?}", e);
                                                return;
                                            }
                                        }
                                    }
                                }
                                
                                // Take the last 'number' entries
                                let start_idx = if entries.len() > number {
                                    entries.len() - number
                                } else {
                                    0
                                };
                                
                                for (i, (key, value)) in entries.iter().skip(start_idx).enumerate() {
                                    match mode {
                                        Mode::Hex => println!("{}: 0x{} -> 0x{}", i + 1, hex::encode(key), hex::encode(value)),
                                        Mode::Str => println!("{}: {} -> {}", i + 1, String::from_utf8_lossy(key), String::from_utf8_lossy(value)),
                                    }
                                }
                                
                                if entries.is_empty() {
                                    println!("No entries found before the given key");
                                }
                            }
                            Ok(None) => println!("Database is empty"),
                            Err(e) => println!("Error: {:?}", e),
                        }
                    } else {
                        println!("Invalid number: {}", args[2]);
                    }
                } else {
                    println!("Invalid key format");
                }
            }
            "above" => {
                if args.len() < 2 {
                    println!("Usage: above <key>");
                    continue;
                }
                if let Some(key_bytes) = parse_arg_bytes(&args[1]) {
                    match file.above(&key_bytes) {
                        Ok(Some(key)) => {
                            match mode {
                                Mode::Hex => println!("Above: 0x{}", hex::encode(&key)),
                                Mode::Str => println!("Above: {}", String::from_utf8_lossy(&key)),
                            }
                        }
                        Ok(None) => println!("No key above the given key"),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Invalid key format");
                }
            }
            "below" => {
                if args.len() < 2 {
                    println!("Usage: below <key>");
                    continue;
                }
                if let Some(key_bytes) = parse_arg_bytes(&args[1]) {
                    match file.below(&key_bytes) {
                        Ok(Some(key)) => {
                            match mode {
                                Mode::Hex => println!("Below: 0x{}", hex::encode(&key)),
                                Mode::Str => println!("Below: {}", String::from_utf8_lossy(&key)),
                            }
                        }
                        Ok(None) => println!("No key below the given key"),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Invalid key format");
                }
            }
            "mode" => {
                if args.len() < 2 {
                    let m = match mode {
                        Mode::Str => "str",
                        Mode::Hex => "hex",
                    };
                    println!("Current mode: '{}'.", m);
                    println!("Usage: mode <str|hex>");
                    continue;
                }
                let m = args[1].to_lowercase();
                if m == "str" {
                    mode = Mode::Str;
                    println!("Mode set to '{}'.", m);
                } else if m == "hex" {
                    mode = Mode::Hex;
                    println!("Mode set to '{}'.", m);
                } else {
                    println!("Unknown mode '{}'. Use 'str' or 'hex'.", args[1]);
                }
            }
            "exit" | "quit" => {
                println!("Exiting.");
                break;
            }
            "help" | "?" => {
                println!("Commands:");
                println!("  lookup <key>       - Get value for key");
                println!("  insert <key> <val> - Set value for key");
                println!("  remove <key>       - Delete key");
                println!("  min                - Show minimum key");
                println!("  max                - Show maximum key");
                println!("  len                - Count total entries (alias: size, count)");
                println!("  from <key> <num>   - Show <num> entries starting after <key> (exclusive)");
                println!("  till <key> <num>   - Show last <num> entries before <key> (exclusive)");
                println!("  above <key>        - Show key above the given key");
                println!("  below <key>        - Show key below the given key");
                println!("  mode <str|hex>     - Set value display mode (str: utf8, hex: 0x...)");
                println!("  exit/quit          - Exit CLI");
                println!("  help               - Show this help");
                println!();
                println!("Key/value arguments can be:");
                println!("  - 0x...            - hex bytes (e.g. 0x68656c6c6f for 'hello')");
                println!("  - \"...\" or '...' - quoted string literal");
                println!("  - plain            - plain string (utf8 bytes)");
            }
                    _ => {
                        println!("Unknown command. Type 'help' for a list of commands.");
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("^D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    
    // Save history before exiting
    let _ = rl.save_history(&history_path);
}
