//! Owned by WP4.3.
//!
//! `docs/CLI.md` is the command reference (REVAMP_PLAN §14 WP4.3). This test
//! keeps it true to the binary:
//!
//! * every `tkfs …` command line inside a fenced shell code block (` ```sh `,
//!   `bash`, `console`, `powershell`, …; ` ```text ` samples of output are
//!   not read) names a real command: its subcommand path is found by walking
//!   the `Commands:` lists of `tkfs [path] --help`, and `tkfs <path> --help`
//!   must succeed;
//! * a word where a subcommand is expected must be one (`tkfs manage bogus`
//!   fails), so a renamed or removed command breaks the test;
//! * every option a documented line uses (`--mount`, `-m`, …) must appear
//!   in that command's help;
//! * every top-level command and every `manage` subcommand of the binary
//!   appears in at least one documented line, so a new command cannot be
//!   left undocumented.
//!
//! Command lines may sit behind a shell prompt (`$ `) or in a pipeline
//! (`echo CODE | tkfs manage pair …`); a trailing ` # comment` is ignored.
//! Placeholders (`STATE`, `<path>`) are positional arguments and are never
//! checked. No daemon is needed: only `--help` runs.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
};

/// The reference this test checks, relative to the crate root.
const CLI_MD: &str = "docs/CLI.md";

/// Options before or between subcommands that take a value
/// (`tkfs [-f CONFIG] [-C DIRECTORY] [--runtime FILE] …`).
const VALUE_OPTIONS: &[&str] = &[
    "-f",
    "--defaults-file",
    "-C",
    "--runtime",
    "--request-id",
    "--generation",
];

/// At least this many command lines must be found: a parser that finds
/// nothing must not pass.
const MIN_DOCUMENTED: usize = 30;

/// One `tkfs` command line of the reference.
#[derive(Debug)]
struct Documented {
    /// 1-based line number in `docs/CLI.md`.
    line: usize,
    text: String,
    /// The words after `tkfs`.
    args: Vec<String>,
}

/// Splits a command line into words: whitespace separates them, and single
/// or double quotes group a word (`"My Project"`). No escapes.
fn words(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut started = false;
    for c in text.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => current.push(c),
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                started = true;
            }
            None if c.is_whitespace() => {
                if started {
                    out.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            None => {
                current.push(c);
                started = true;
            }
        }
    }
    if started {
        out.push(current);
    }
    out
}

/// Info strings of the fenced blocks that hold commands. Other blocks
/// (`text` samples of output, `toml`, `json`) are not read.
const SHELL_BLOCKS: &[&str] = &["sh", "shell", "bash", "console", "powershell", "pwsh"];

/// Every `tkfs` command line inside a fenced shell code block of `markdown`.
fn documented_commands(markdown: &str) -> Vec<Documented> {
    let mut out = Vec::new();
    // `Some(shell)` inside a fenced block.
    let mut block: Option<bool> = None;
    for (index, raw) in markdown.lines().enumerate() {
        let trimmed = raw.trim_start();
        if let Some(info) = trimmed.strip_prefix("```") {
            block = match block {
                Some(_) => None,
                None => Some(SHELL_BLOCKS.contains(&info.trim())),
            };
            continue;
        }
        if block != Some(true) {
            continue;
        }
        // A trailing comment is not part of the command.
        let code = match trimmed.find(" #") {
            Some(at) => &trimmed[..at],
            None => trimmed,
        };
        let code = code.strip_prefix("$ ").unwrap_or(code);
        // Each stage of a pipeline or `&&` chain that runs tkfs.
        for stage in code.split(" | ").flat_map(|part| part.split(" && ")) {
            let mut args = words(stage);
            if args.first().map(String::as_str) != Some("tkfs") {
                continue;
            }
            args.remove(0);
            out.push(Documented {
                line: index + 1,
                text: stage.trim().to_owned(),
                args,
            });
        }
    }
    out
}

/// `tkfs <path> --help`, cached per path: (succeeded, stdout + stderr).
struct Helper {
    exe: &'static str,
    cache: BTreeMap<Vec<String>, (bool, String)>,
}

impl Helper {
    fn new() -> Self {
        Self {
            exe: env!("CARGO_BIN_EXE_tkfs"),
            cache: BTreeMap::new(),
        }
    }

    fn help(&mut self, path: &[String]) -> (bool, String) {
        if let Some(found) = self.cache.get(path) {
            return found.clone();
        }
        let output = Command::new(self.exe)
            .args(path)
            .arg("--help")
            .output()
            .expect("run tkfs --help");
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        let found = (output.status.success(), text);
        self.cache.insert(path.to_vec(), found.clone());
        found
    }

    /// The subcommands `tkfs <path> --help` lists (without `help`).
    fn subcommands(&mut self, path: &[String]) -> BTreeSet<String> {
        let (_, text) = self.help(path);
        commands_of(&text)
    }
}

/// The names of the `Commands:` section of a clap help text.
fn commands_of(help: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut in_commands = false;
    for line in help.lines() {
        if line.trim_end() == "Commands:" {
            in_commands = true;
            continue;
        }
        if !in_commands {
            continue;
        }
        if line.trim().is_empty() {
            break;
        }
        // An entry is indented by two spaces; a wrapped description by more.
        let Some(entry) = line.strip_prefix("  ") else {
            break;
        };
        if entry.starts_with(' ') {
            continue;
        }
        if let Some(name) = entry.split_whitespace().next()
            && name != "help"
        {
            out.insert(name.to_owned());
        }
    }
    out
}

/// Whether `help` lists `option` (`--mount`, or a short `-m`).
fn lists_option(help: &str, option: &str) -> bool {
    help.match_indices(option).any(|(at, _)| {
        let before = help[..at].chars().next_back();
        let after = help[at + option.len()..].chars().next();
        let starts = before.is_none_or(|c| c.is_whitespace() || c == '[' || c == ',');
        let ends = after.is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        starts && ends
    })
}

/// What a documented line means: its subcommand path and the options it
/// uses after `tkfs`, or why it names no command.
fn resolve(helper: &mut Helper, doc: &Documented) -> Result<(Vec<String>, Vec<String>), String> {
    let mut path: Vec<String> = Vec::new();
    let mut options = Vec::new();
    let mut listed = helper.subcommands(&path);
    let mut words = doc.args.iter().peekable();
    while let Some(word) = words.next() {
        if word.starts_with('-') && word.len() > 1 {
            let name = word.split('=').next().unwrap_or(word).to_owned();
            if VALUE_OPTIONS.contains(&name.as_str()) && !word.contains('=') {
                words.next();
            }
            options.push(name);
            continue;
        }
        if listed.contains(word) {
            path.push(word.clone());
            listed = helper.subcommands(&path);
            continue;
        }
        if !listed.is_empty() {
            // A subcommand is expected here: a hidden alias (`start`) has a
            // help of its own; anything else is not a command.
            let mut alias = path.clone();
            alias.push(word.clone());
            if helper.help(&alias).0 {
                path = alias;
                listed = helper.subcommands(&path);
                continue;
            }
            return Err(format!(
                "`{word}` is not a subcommand of `tkfs {}` (it has: {})",
                path.join(" "),
                listed.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        // A positional argument of the leaf command (a placeholder).
    }
    let asks_help = options.iter().any(|o| o == "--help" || o == "-h");
    if !listed.is_empty() && !asks_help && !doc.args.is_empty() {
        return Err(format!(
            "`tkfs {}` needs a subcommand ({})",
            path.join(" "),
            listed.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok((path, options))
}

/// Checks one documented line against the binary: its command path exists
/// (`tkfs <path> --help` succeeds) and that help lists every option the line
/// uses. Returns the path.
fn check(helper: &mut Helper, doc: &Documented) -> Result<Vec<String>, String> {
    let (path, options) = resolve(helper, doc)?;
    let (ok, help) = helper.help(&path);
    if !ok {
        return Err(format!("`tkfs {} --help` failed:\n{help}", path.join(" ")));
    }
    let missing: Vec<&String> = options
        .iter()
        .filter(|o| *o != "--help" && *o != "-h")
        .filter(|o| !lists_option(&help, o))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "`tkfs {} --help` does not list {}",
            path.join(" "),
            missing
                .iter()
                .map(|o| o.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(path)
}

/// A documented line built from words, for the checker's own tests.
fn line(text: &str) -> Documented {
    Documented {
        line: 1,
        text: text.to_owned(),
        args: words(text).into_iter().skip(1).collect(),
    }
}

#[test]
fn every_documented_command_exists_with_its_options() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let markdown = std::fs::read_to_string(root.join(CLI_MD)).expect("read docs/CLI.md");
    let documented = documented_commands(&markdown);
    assert!(
        documented.len() >= MIN_DOCUMENTED,
        "only {} `tkfs` command lines found in {CLI_MD} code blocks",
        documented.len()
    );
    let mut helper = Helper::new();
    let mut problems = Vec::new();
    let mut paths: BTreeSet<Vec<String>> = BTreeSet::new();
    for doc in &documented {
        match check(&mut helper, doc) {
            Ok(path) => {
                paths.insert(path);
            }
            Err(why) => problems.push(format!("{CLI_MD}:{}: `{}`: {why}", doc.line, doc.text)),
        }
    }

    // Every command of the binary is documented.
    let top = helper.subcommands(&[]);
    assert!(
        top.contains("manage"),
        "`tkfs --help` lists no `manage` command: {top:?}"
    );
    for command in &top {
        if !paths.iter().any(|path| path.first() == Some(command)) {
            problems.push(format!(
                "`tkfs {command}` is not documented in a {CLI_MD} code block"
            ));
        }
    }
    let manage = vec!["manage".to_owned()];
    for command in helper.subcommands(&manage) {
        let path = vec!["manage".to_owned(), command.clone()];
        if !paths.contains(&path) {
            problems.push(format!(
                "`tkfs manage {command}` is not documented in a {CLI_MD} code block"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "{CLI_MD} disagrees with the binary:\n{}",
        problems.join("\n")
    );
}

#[test]
fn the_reference_parser_reads_prompts_pipelines_and_quotes() {
    let markdown = "text tkfs manage list (prose is ignored)\n\
                    ```sh\n\
                    $ tkfs manage create \"My Project\" --headless # a comment\n\
                    echo CODE | tkfs manage pair wss://host.example\n\
                    ls && tkfs manage list\n\
                    ```\n\
                    tkfs outside a block\n\
                    ```text\n\
                    tkfs manage watch · updates live (sample output is ignored)\n\
                    ```\n";
    let found = documented_commands(markdown);
    let args: Vec<Vec<&str>> = found
        .iter()
        .map(|d| d.args.iter().map(String::as_str).collect())
        .collect();
    assert_eq!(
        args,
        vec![
            vec!["manage", "create", "My Project", "--headless"],
            vec!["manage", "pair", "wss://host.example"],
            vec!["manage", "list"],
        ]
    );
    assert_eq!(found[0].line, 3);
}

#[test]
fn the_help_parser_reads_clap_command_lists_and_options() {
    let help = "Usage: tkfs manage <COMMAND>\n\n\
                Commands:\n  \
                status              Show the daemon\n                      \
                and the peers.\n  \
                configure-mount     Set or clear a mount path\n  \
                help                Print this message\n\n\
                Options:\n      \
                --json  Print JSON\n  \
                -m, --message <MESSAGE>\n";
    let commands: Vec<String> = commands_of(help).into_iter().collect();
    assert_eq!(commands, vec!["configure-mount", "status"]);
    assert!(lists_option(help, "--json"));
    assert!(lists_option(help, "-m"));
    assert!(lists_option(help, "--message"));
    assert!(!lists_option(help, "--mess"));
    assert!(!lists_option(help, "--mount"));
}

#[test]
fn the_checker_refuses_unknown_commands_options_and_incomplete_lines() {
    let mut helper = Helper::new();
    // Real commands pass, with global options before or after the command,
    // a hidden alias and placeholders.
    for good in [
        "tkfs -f orchestrator.toml manage stop STATE --no-wait",
        "tkfs manage status --json",
        "tkfs -C MOUNT checkpoint -m MESSAGE",
        "tkfs --runtime STATE/runtime.json status",
        "tkfs start",
    ] {
        assert!(check(&mut helper, &line(good)).is_ok(), "{good}");
    }
    assert_eq!(
        check(&mut helper, &line("tkfs manage stop STATE")).unwrap(),
        vec!["manage".to_owned(), "stop".to_owned()]
    );
    // A command that does not exist, an option a command does not have, and
    // a command group without its subcommand are refused.
    for bad in [
        "tkfs manage bogus STATE",
        "tkfs bogus",
        "tkfs manage stop STATE --bogus",
        "tkfs manage pause STATE --force",
        "tkfs checkpoint --message-text MESSAGE",
        "tkfs manage",
    ] {
        assert!(check(&mut helper, &line(bad)).is_err(), "{bad}");
    }
}
