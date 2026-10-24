use std::env;
use std::path::Path;
use std::process::Command;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let task = args.next().unwrap_or_else(|| "help".into());
    if args.next().is_some() {
        eprintln!("Expected one task: fmt, test, clippy, or ci");
        return ExitCode::FAILURE;
    }
    let tasks: &[&str] = match task.as_str() {
        "fmt" => &["fmt"],
        "test" => &["test"],
        "clippy" => &["clippy"],
        "ci" => &["fmt", "test", "clippy"],
        "help" | "-h" | "--help" => {
            println!("Usage: cargo xtask [fmt|test|clippy|ci]");
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("Unknown task: {task}");
            return ExitCode::FAILURE;
        }
    };
    for task in tasks {
        let args: &[&str] = match *task {
            "fmt" => &["+nightly", "fmt", "--all", "--check"],
            "test" => &["test", "--workspace"],
            "clippy" => &[
                "clippy",
                "--workspace",
                "--all-targets",
                "--no-deps",
                "--",
                "-D",
                "warnings",
            ],
            _ => unreachable!(),
        };
        let status = Command::new("cargo")
            .args(args)
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
            .status();
        match status {
            Ok(status) if status.success() => {}
            Ok(_) => return ExitCode::FAILURE,
            Err(error) => {
                eprintln!("Failed to run {task}: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
