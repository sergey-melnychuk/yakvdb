use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::io::{self, Write};
use std::path::Path;

use yakvdb::api::page::Page;
use yakvdb::api::Store;
use yakvdb::disk::block::Block;
use yakvdb::disk::file::File;

fn main() {
    // Handle broken pipe errors silently
    std::panic::set_hook(Box::new(|info| {
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            if s.contains("Broken pipe") {
                std::process::exit(0);
            }
        }
        if let Some(s) = info.payload().downcast_ref::<String>() {
            if s.contains("Broken pipe") {
                std::process::exit(0);
            }
        }
    }));

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("Usage: yak <path>");
        std::process::exit(1);
    }
    let path = Path::new(&args[1]);
    let file = if !path.exists() {
        File::<Block>::make(path, 4096)
    } else {
        File::<Block>::open(path)
    };
    let Ok(file) = file else {
        eprintln!("Failed to open file: {}", path.display());
        std::process::exit(1);
    };

    // Calculate total pages from file size
    let Ok(file_meta) = std::fs::metadata(path) else {
        eprintln!("Failed to read file metadata: {}", path.display());
        std::process::exit(1);
    };
    let file_size = file_meta.len();
    let head_size = 16; // MAGIC(8) + HEAD(8)
    let page_size = file.page_size() as u64;
    let page_count = ((file_size - head_size) / page_size) as u32;

    println!("YAK CLI started. Type 'help' for commands. Type 'exit' to quit.");
    println!("Page size: {page_size} bytes. Total pages: {page_count}.");

    // Helper to parse a single argument as bytes, supporting:
    // - 0x... hex
    // - "...", '...' quoted string
    // - plain string
    fn parse_arg_bytes(arg: &str) -> Option<Vec<u8>> {
        let arg = arg.trim();
        if arg.starts_with("0x") || arg.starts_with("0X") {
            hex::decode(&arg[2..]).ok()
        } else if (arg.starts_with('"') && arg.ends_with('"'))
            || (arg.starts_with('\'') && arg.ends_with('\''))
        {
            Some(arg.as_bytes()[1..arg.len() - 1].to_vec())
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
            } else if c == '"' || c == '\'' {
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
                                Ok(Some(val)) => match mode {
                                    Mode::Hex => println!("0x{}", hex::encode(&val)),
                                    Mode::Str => println!("\"{}\"", String::from_utf8_lossy(&val)),
                                },
                                Ok(None) => {
                                    println!("Key not found");
                                }
                                Err(e) => {
                                    println!("Error: {e:?}");
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
                            Err(e) => println!("Error: {e:?}"),
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
                                Err(e) => println!("Error: {e:?}"),
                            }
                        } else {
                            println!("Invalid key format");
                        }
                    }
                    "min" => match file.min() {
                        Ok(Some(key)) => match mode {
                            Mode::Hex => println!("0x{}", hex::encode(&key)),
                            Mode::Str => println!("\"{}\"", String::from_utf8_lossy(&key)),
                        },
                        Ok(None) => println!("No minimum key (empty database)"),
                        Err(e) => println!("Error: {e:?}"),
                    },
                    "max" => match file.max() {
                        Ok(Some(key)) => match mode {
                            Mode::Hex => println!("0x{}", hex::encode(&key)),
                            Mode::Str => println!("\"{}\"", String::from_utf8_lossy(&key)),
                        },
                        Ok(None) => println!("No maximum key (empty database)"),
                        Err(e) => println!("Error: {e:?}"),
                    },
                    "len" | "size" | "count" => {
                        let mut len = 0u64;
                        for id in 1..=page_count {
                            if let Some(page) = file.read_page(id) {
                                len += page.len() as u64;
                            }
                        }
                        println!("Total entries: {len}");
                    }
                    "from" => {
                        if args.len() < 3 {
                            println!("Usage: from <key> <number>");
                            continue;
                        }
                        if let Some(key) = parse_arg_bytes(&args[1]) {
                            if let Ok(number) = args[2].parse::<usize>() {
                                if number == 0 {
                                    println!("Number must be greater than 0");
                                    continue;
                                }

                                let mut entries = Vec::new();
                                let mut current_key = key;
                                while entries.len() < number {
                                    match file.above(&current_key) {
                                        Ok(Some(next_key)) => {
                                            if let Ok(Some(value)) = file.lookup(&next_key) {
                                                entries.push((next_key.clone(), value));
                                            }
                                            current_key = next_key;
                                        }
                                        Ok(None) => break,
                                        Err(e) => {
                                            println!("Error during iteration: {e:?}");
                                            return;
                                        }
                                    }
                                }

                                for (key, value) in entries.iter() {
                                    match mode {
                                        Mode::Hex => println!(
                                            "0x{} : 0x{}",
                                            hex::encode(key),
                                            hex::encode(value)
                                        ),
                                        Mode::Str => println!(
                                            "\"{}\" : \"{}\"",
                                            String::from_utf8_lossy(key),
                                            String::from_utf8_lossy(value)
                                        ),
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
                        if let Some(key) = parse_arg_bytes(&args[1]) {
                            if let Ok(number) = args[2].parse::<usize>() {
                                if number == 0 {
                                    println!("Number must be greater than 0");
                                    continue;
                                }

                                let mut entries = Vec::new();
                                let mut current_key = key;
                                while entries.len() < number {
                                    match file.below(&current_key) {
                                        Ok(Some(next_key)) => {
                                            if let Ok(Some(value)) = file.lookup(&next_key) {
                                                entries.push((next_key.clone(), value));
                                            }
                                            current_key = next_key;
                                        }
                                        Ok(None) => break,
                                        Err(e) => {
                                            println!("Error during iteration: {e:?}");
                                            return;
                                        }
                                    }
                                }

                                for (key, value) in entries.iter().rev() {
                                    match mode {
                                        Mode::Hex => println!(
                                            "0x{} : 0x{}",
                                            hex::encode(key),
                                            hex::encode(value)
                                        ),
                                        Mode::Str => println!(
                                            "\"{}\" : \"{}\"",
                                            String::from_utf8_lossy(key),
                                            String::from_utf8_lossy(value)
                                        ),
                                    }
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
                                Ok(Some(key)) => match mode {
                                    Mode::Hex => println!("0x{}", hex::encode(&key)),
                                    Mode::Str => println!("\"{}\"", String::from_utf8_lossy(&key)),
                                },
                                Ok(None) => println!("No key above the given key"),
                                Err(e) => println!("Error: {e:?}"),
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
                                Ok(Some(key)) => match mode {
                                    Mode::Hex => println!("0x{}", hex::encode(&key)),
                                    Mode::Str => println!("\"{}\"", String::from_utf8_lossy(&key)),
                                },
                                Ok(None) => println!("No key below the given key"),
                                Err(e) => println!("Error: {e:?}"),
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
                            println!("Current mode: '{m}'.");
                            println!("Usage: mode <str|hex>");
                            continue;
                        }
                        let m = args[1].to_lowercase();
                        if m == "str" {
                            mode = Mode::Str;
                            println!("Mode set to '{m}'.");
                        } else if m == "hex" {
                            mode = Mode::Hex;
                            println!("Mode set to '{m}'.");
                        } else {
                            println!("Unknown mode '{}'. Use 'str' or 'hex'.", args[1]);
                        }
                    }
                    "free" => {
                        let mut size = 0u64;
                        let mut free = 0u64;
                        for id in 1..=page_count {
                            if let Some(page) = file.read_page(id) {
                                size += page.cap() as u64;
                                free += page.free() as u64;
                            }
                        }
                        println!("Size: {size}");
                        println!("Used: {}", size - free);
                        println!("Free: {free}");
                    }
                    "tree" => {
                        // Helper function to determine page type
                        fn page_type(file: &File<Block>, id: u32) -> &'static str {
                            if let Some(page) = file.read_page(id) {
                                if page.len() == 0 {
                                    return "EMPTY";
                                }
                                // Check if all slots are leaf entries (page_ref == 0)
                                let entries = page.copy();
                                for (_, _, page_ref, raw_vlen) in entries {
                                    if page_ref > 0 && raw_vlen & yakvdb::api::page::OVERFLOW_FLAG == 0 {
                                        return "NODE";
                                    }
                                }
                                "LEAF"
                            } else {
                                "INVALID"
                            }
                        }

                        // Collect all output lines for pagination
                        let mut output_lines = Vec::new();
                        output_lines.push("B-Tree Structure:".to_string());
                        output_lines.push("=================".to_string());
                        output_lines.push(format!(
                            "{:<6} {:<8} {:<6} {:<20}",
                            "ID", "TYPE", "FILL%", "KEYS"
                        ));
                        output_lines.push(format!("{:-<45}", ""));

                        for id in 1..=page_count {
                            if let Some(page) = file.read_page(id) {
                                let page_type = page_type(&file, id);
                                let fill_percent = page.full();

                                let key_range = if page.len() > 0 {
                                    let min_key = match mode {
                                        Mode::Hex => format!("0x{}", hex::encode(page.min())),
                                        Mode::Str => {
                                            format!("\"{}\"", String::from_utf8_lossy(page.min()))
                                        }
                                    };
                                    let max_key = match mode {
                                        Mode::Hex => format!("0x{}", hex::encode(page.max())),
                                        Mode::Str => {
                                            format!("\"{}\"", String::from_utf8_lossy(page.max()))
                                        }
                                    };
                                    if min_key == max_key {
                                        min_key.to_string()
                                    } else {
                                        format!("{min_key}..{max_key}")
                                    }
                                } else {
                                    "empty".to_string()
                                };

                                let truncated_range = if key_range.len() > 18 {
                                    format!("{}...", &key_range[..15])
                                } else {
                                    key_range
                                };

                                output_lines.push(format!("{id:<6} {page_type:<8} {fill_percent:<6} {truncated_range:<20}"));
                            }
                        }

                        // Use pagination for long output (20 lines per page)
                        paginated_print(&output_lines, 20);
                    }
                    "page" => {
                        if args.len() < 2 {
                            println!("Usage: page <id>");
                            continue;
                        }
                        if let Ok(page_id) = args[1].parse::<u32>() {
                            if let Some(page) = file.read_page(page_id) {
                                // Determine page type
                                let page_type = if page.len() == 0 {
                                    "EMPTY"
                                } else {
                                    let entries = page.copy();
                                    let mut is_leaf = true;
                                    for (_, _, page_ref, raw_vlen) in entries {
                                        if page_ref > 0 && raw_vlen & yakvdb::api::page::OVERFLOW_FLAG == 0 {
                                            is_leaf = false;
                                            break;
                                        }
                                    }
                                    if is_leaf {
                                        "LEAF"
                                    } else {
                                        "NODE"
                                    }
                                };

                                println!("Page Details:");
                                println!("=============");
                                println!("ID: {page_id}");
                                println!("Type: {page_type}");
                                println!("Fill: {}%", page.full());
                                println!("Capacity: {} bytes", page.cap());
                                println!("Free: {} bytes", page.free());
                                println!("Entries: {}", page.len());

                                if page.len() > 0 {
                                    println!("\nEntries:");
                                    println!("{:<4} {:<20} {:<20}", "IDX", "KEY", "VALUE");
                                    println!("{:-<44}", "");

                                    let entries = page.copy();
                                    for (i, (key, val, page_ref, raw_vlen)) in entries.iter().enumerate() {
                                        let key_str = match mode {
                                            Mode::Hex => format!("0x{}", hex::encode(key)),
                                            Mode::Str => {
                                                format!("\"{}\"", String::from_utf8_lossy(key))
                                            }
                                        };
                                        let val_str = if val.is_empty() {
                                            "-".to_string()
                                        } else {
                                            match mode {
                                                Mode::Hex => format!("0x{}", hex::encode(val)),
                                                Mode::Str => {
                                                    format!("\"{}\"", String::from_utf8_lossy(val))
                                                }
                                            }
                                        };

                                        let truncated_key = if key_str.len() > 18 {
                                            format!("{}...", &key_str[..15])
                                        } else {
                                            key_str
                                        };

                                        let truncated_val = if val_str.len() > 18 {
                                            format!("{}...", &val_str[..15])
                                        } else {
                                            val_str
                                        };

                                        let val_or_page_ref = if *raw_vlen & yakvdb::api::page::OVERFLOW_FLAG != 0 {
                                            let m = raw_vlen & !yakvdb::api::page::OVERFLOW_FLAG;
                                            format!("[OVERFLOW: {m} pages, PAGE: {page_ref}]")
                                        } else if *page_ref > 0 {
                                            format!("[PAGE: {page_ref}]")
                                        } else {
                                            truncated_val
                                        };

                                        println!(
                                            "{i:<4} {truncated_key:<20} {val_or_page_ref:<20}"
                                        );
                                    }
                                } else {
                                    println!("\nPage is empty");
                                }
                            } else {
                                println!("Page {page_id} not found or cannot be loaded");
                            }
                        } else {
                            println!("Invalid page ID: {}", args[1]);
                        }
                    }
                    "root" => {
                        let Some(root_page) = file.read_root() else {
                            println!("Root page could not be read");
                            continue;
                        };
                        let page_type = if root_page.len() == 0 {
                            "EMPTY"
                        } else {
                            let entries = root_page.copy();
                            let mut is_leaf = true;
                            for (_, _, page_ref, raw_vlen) in entries {
                                if page_ref > 0 && raw_vlen & yakvdb::api::page::OVERFLOW_FLAG == 0 {
                                    is_leaf = false;
                                    break;
                                }
                            }
                            if is_leaf {
                                "LEAF"
                            } else {
                                "NODE"
                            }
                        };

                        println!("Root Page (ID: 1):");
                        println!("==================");
                        println!("Type: {page_type}");
                        println!("Fill: {}%", root_page.full());
                        println!("Capacity: {} bytes", root_page.cap());
                        println!("Free: {} bytes", root_page.free());
                        println!("Entries: {}", root_page.len());

                        if root_page.len() > 0 {
                            println!("\nEntries:");
                            println!("{:<4} {:<20} {:<20}", "IDX", "KEY", "VALUE");
                            println!("{:-<44}", "");

                            let entries = root_page.copy();
                            for (i, (key, val, page_ref, raw_vlen)) in entries.iter().enumerate() {
                                let key_str = match mode {
                                    Mode::Hex => format!("0x{}", hex::encode(key)),
                                    Mode::Str => format!("\"{}\"", String::from_utf8_lossy(key)),
                                };
                                let val_str = if val.is_empty() {
                                    "-".to_string()
                                } else {
                                    match mode {
                                        Mode::Hex => format!("0x{}", hex::encode(val)),
                                        Mode::Str => {
                                            format!("\"{}\"", String::from_utf8_lossy(val))
                                        }
                                    }
                                };

                                let truncated_key = if key_str.len() > 18 {
                                    format!("{}...", &key_str[..15])
                                } else {
                                    key_str
                                };

                                let truncated_val = if val_str.len() > 18 {
                                    format!("{}...", &val_str[..15])
                                } else {
                                    val_str
                                };

                                let val_or_page_ref = if *raw_vlen & yakvdb::api::page::OVERFLOW_FLAG != 0 {
                                    let m = raw_vlen & !yakvdb::api::page::OVERFLOW_FLAG;
                                    format!("[OVERFLOW: {m} pages, PAGE: {page_ref}]")
                                } else if *page_ref > 0 {
                                    format!("[PAGE: {page_ref}]")
                                } else {
                                    truncated_val
                                };

                                println!("{i:<4} {truncated_key:<20} {val_or_page_ref:<20}");
                            }
                        } else {
                            println!("\nRoot page is empty");
                        }
                    }
                    "explain" => {
                        // TODO: explain - show lookup path for the key
                        println!("I'm in no mood to explain anything.");
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
                        println!(
                            "  mode <str|hex>     - Set value display mode (str: utf8, hex: 0x...)"
                        );
                        println!("  tree               - Show B-tree structure (pages, types, fill ratios)");
                        println!("  page <id>          - Show detailed page information");
                        println!("  root               - Show root page details");
                        println!("  exit/quit          - Exit CLI");
                        println!("  help/?             - Show this help");
                        println!();
                        println!("Key/value arguments can be:");
                        println!(
                            "  - 0x...            - hex bytes (e.g. 0x68656c6c6f for 'hello')"
                        );
                        println!("  - \"...\" or '...'   - quoted string literal");
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
                println!("Error: {err:?}");
                break;
            }
        }
    }

    // Save history before exiting
    let _ = rl.save_history(&history_path);
}

// Simple pagination helper - only paginate if output is to terminal
fn paginated_print(lines: &[String], lines_per_page: usize) {
    // If output is not to a terminal (piped/redirected), just print all lines
    if !atty::is(atty::Stream::Stdout) {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        for line in lines {
            // Handle broken pipe gracefully (when piped to head, less, etc.)
            if writeln!(handle, "{line}").is_err() {
                return; // Exit silently on broken pipe
            }
        }
        return;
    }

    // If output is short enough, just print it all
    if lines.len() <= lines_per_page {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        for line in lines {
            if writeln!(handle, "{line}").is_err() {
                return; // Exit silently on broken pipe
            }
        }
        return;
    }

    // Interactive pagination for long output to terminal
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    for chunk in lines.chunks(lines_per_page) {
        for line in chunk {
            if writeln!(handle, "{line}").is_err() {
                return; // Exit silently on broken pipe
            }
        }

        if chunk.len() == lines_per_page {
            static MESSAGE: &str = "-- More -- (Press any key to continue, 'q' to quit): ";
            if write!(handle, "{MESSAGE}").is_err() {
                return; // Exit silently on broken pipe
            }
            if write!(handle, "\r{}\r", " ".repeat(MESSAGE.len())).is_ok() {
                let _ = handle.flush();
            }
            if handle.flush().is_err() {
                return; // Exit silently on broken pipe
            }

            // Enable raw mode for single character input
            if enable_raw_mode().is_ok() {
                // Wait for a single key press
                if let Ok(Event::Key(key_event)) = event::read() {
                    let _ = disable_raw_mode();
                    match key_event.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                            // Clear the prompt line
                            if write!(handle, "\r{}\r", " ".repeat(50)).is_ok() {
                                let _ = handle.flush();
                            }
                            break;
                        }
                        _ => {
                            // Clear the prompt line and continue
                            if write!(handle, "\r{}\r", " ".repeat(50)).is_ok() {
                                let _ = handle.flush();
                            }
                        }
                    }
                } else {
                    let _ = disable_raw_mode();
                }
            }
        }
    }
}
