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
//! * every option a documented line uses (`--mount`, `-m`, …) must be an
//!   entry of the `Options:` section of that command's help (a name that
//!   only appears in another option's description does not count), and the
//!   words after an option that takes a value are that value;
//! * the line gives the positional arguments the command's `Usage:` line
//!   asks for (`<ENDPOINT> <REPO> <LABEL>`: three; `[NAME]`: optional;
//!   `<NAME>...`: at least one) and every option that line requires
//!   (`--state <STATE>`, by its long or short name);
//! * every top-level command and every `manage` subcommand of the binary
//!   appears in at least one documented line, so a new command cannot be
//!   left undocumented.
//!
//! Command lines may sit behind a shell prompt (`$ `) or in a pipeline
//! (`echo CODE | tkfs manage pair …`); a trailing ` # comment` is ignored.
//! Placeholders (`STATE`, `<path>`) are positional arguments: they are
//! counted, never interpreted. No daemon is needed: only `--help` runs.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
};

/// The reference this test checks, relative to the crate root.
const CLI_MD: &str = "docs/CLI.md";

/// Options before or between subcommands that take a value
/// (`tkfs [-f CONFIG] [-C DIRECTORY] [--runtime FILE] …`): the fallback when
/// the help of the command reached so far does not describe an option.
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

/// How many values an option takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Takes {
    /// A flag (`--headless`).
    Nothing,
    /// One value per occurrence (`--mount <MOUNT>`).
    One,
    /// Every following word up to the next option (`--conflict <ID>...`,
    /// clap's `num_args = 1..`).
    Many,
}

/// One entry of the `Options:` section of a clap help text.
#[derive(Debug, PartialEq, Eq)]
struct OptionSpec {
    /// Its names: `["-f", "--defaults-file"]`, `["-C"]`, `["--json"]`.
    names: Vec<String>,
    takes: Takes,
}

/// The entries of the `Options:` section of a clap help text. An entry
/// starts with `  -x` (a short name) or `      --name` (long only); the
/// description follows after two spaces, and wrapped description lines are
/// indented further, so a name in a description is never an entry.
fn options_of(help: &str) -> Vec<OptionSpec> {
    let mut out = Vec::new();
    let mut in_options = false;
    for line in help.lines() {
        if line.trim_end() == "Options:" {
            in_options = true;
            continue;
        }
        if !in_options {
            continue;
        }
        if !line.is_empty() && !line.starts_with(' ') {
            // The next section, or text after the options.
            break;
        }
        if !(line.starts_with("  -") || line.starts_with("      --")) {
            continue;
        }
        // The names and the value, before the description's column.
        let spec = line.trim_start().split("  ").next().unwrap_or_default();
        let mut names = Vec::new();
        let mut takes = Takes::Nothing;
        for token in spec.split_whitespace() {
            let token = token.trim_end_matches(',');
            if token.starts_with('-') {
                names.push(token.to_owned());
                continue;
            }
            if token.starts_with('<') || token.starts_with('[') {
                takes = if token.ends_with("...") {
                    Takes::Many
                } else {
                    Takes::One
                };
            }
            break;
        }
        if !names.is_empty() {
            out.push(OptionSpec { names, takes });
        }
    }
    out
}

/// The option of `specs` that has the name `name`.
fn find_option<'a>(specs: &'a [OptionSpec], name: &str) -> Option<&'a OptionSpec> {
    specs
        .iter()
        .find(|spec| spec.names.iter().any(|n| n == name))
}

/// What the `Usage:` line of a clap help asks for.
#[derive(Debug, Default, PartialEq, Eq)]
struct Usage {
    /// Required positional arguments (`<ENDPOINT>`).
    required: usize,
    /// Optional positional arguments (`[NAME]`).
    optional: usize,
    /// The last positional takes any number of values (`<NAME>...`).
    variadic: bool,
    /// Options the line requires (`--state <STATE>`), by one of their names.
    required_options: Vec<String>,
}

/// Reads the first `Usage:` line of a clap help: `[OPTIONS]` and
/// `<COMMAND>` are skipped, an option there is required (with its value,
/// unless the `Options:` section says it is a flag: in
/// `--current-state-only <NAME>`, `<NAME>` is a positional argument), and
/// the other bracketed words are positional arguments.
fn usage_of(help: &str) -> Usage {
    let mut usage = Usage::default();
    let Some(line) = help.lines().find_map(|l| l.trim().strip_prefix("Usage:")) else {
        return usage;
    };
    let specs = options_of(help);
    let mut tokens = line.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        if matches!(token, "[OPTIONS]" | "<COMMAND>" | "[COMMAND]") {
            continue;
        }
        if token.starts_with('-') {
            usage.required_options.push(token.to_owned());
            // Its value, if it takes one.
            if find_option(&specs, token).is_none_or(|spec| spec.takes != Takes::Nothing) {
                tokens.next_if(|next| next.starts_with('<'));
            }
            continue;
        }
        if token.starts_with('<') {
            usage.required += 1;
        } else if token.starts_with('[') {
            usage.optional += 1;
        } else {
            // The program name and the subcommand path.
            continue;
        }
        usage.variadic |= token.ends_with("...");
    }
    usage
}

/// What a documented line means: its subcommand path, the options it uses
/// after `tkfs` and the leaf command's positional arguments.
#[derive(Debug)]
struct Resolved {
    path: Vec<String>,
    options: Vec<String>,
    positionals: Vec<String>,
}

/// Whether a word of a command line is an option (`-m`, `--mount=X`).
fn is_option(word: &str) -> bool {
    word.starts_with('-') && word.len() > 1
}

/// Reads a documented line word by word: options (and their values, as the
/// help of the command reached so far describes them), subcommands, and the
/// leaf command's positional arguments; or says why it names no command.
fn resolve(helper: &mut Helper, doc: &Documented) -> Result<Resolved, String> {
    let mut path: Vec<String> = Vec::new();
    let mut options = Vec::new();
    let mut positionals = Vec::new();
    let mut listed = helper.subcommands(&path);
    let mut words = doc.args.iter().peekable();
    while let Some(word) = words.next() {
        if is_option(word) {
            let name = word.split('=').next().unwrap_or(word).to_owned();
            let takes = if word.contains('=') {
                Takes::Nothing
            } else {
                let (_, help) = helper.help(&path);
                match find_option(&options_of(&help), &name) {
                    Some(spec) => spec.takes,
                    None if VALUE_OPTIONS.contains(&name.as_str()) => Takes::One,
                    None => Takes::Nothing,
                }
            };
            match takes {
                Takes::Nothing => {}
                Takes::One => {
                    words.next();
                }
                Takes::Many => while words.next_if(|next| !is_option(next)).is_some() {},
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
        positionals.push(word.clone());
    }
    let asks_help = options.iter().any(|o| o == "--help" || o == "-h");
    if !listed.is_empty() && !asks_help && !doc.args.is_empty() {
        return Err(format!(
            "`tkfs {}` needs a subcommand ({})",
            path.join(" "),
            listed.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(Resolved {
        path,
        options,
        positionals,
    })
}

/// Checks one documented line against the binary: its command path exists
/// (`tkfs <path> --help` succeeds), that help's `Options:` section lists
/// every option the line uses, and the line gives the positional arguments
/// and the options the help's `Usage:` line requires. Returns the path.
fn check(helper: &mut Helper, doc: &Documented) -> Result<Vec<String>, String> {
    let Resolved {
        path,
        options,
        positionals,
    } = resolve(helper, doc)?;
    let (ok, help) = helper.help(&path);
    if !ok {
        return Err(format!("`tkfs {} --help` failed:\n{help}", path.join(" ")));
    }
    let command = format!("tkfs {}", path.join(" "));
    let specs = options_of(&help);
    let missing: Vec<&str> = options
        .iter()
        .map(String::as_str)
        .filter(|o| find_option(&specs, o).is_none())
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "`{command} --help` does not list {}",
            missing.join(", ")
        ));
    }
    if options.iter().any(|o| o == "--help" || o == "-h") {
        return Ok(path);
    }
    let usage = usage_of(&help);
    let given = positionals.len();
    if given < usage.required {
        return Err(format!(
            "`{command}` needs {} positional argument(s), the line gives {given}",
            usage.required
        ));
    }
    if !usage.variadic && given > usage.required + usage.optional {
        return Err(format!(
            "`{command}` takes at most {} positional argument(s), the line gives {given} ({})",
            usage.required + usage.optional,
            positionals.join(" ")
        ));
    }
    for required in &usage.required_options {
        let names = find_option(&specs, required)
            .map_or_else(|| vec![required.clone()], |spec| spec.names.clone());
        if !options.iter().any(|o| names.contains(o)) {
            return Err(format!("`{command}` requires {required}"));
        }
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
                --json                    Print JSON, unlike --mount or -x\n                                \
                --wrapped begins this description line\n  \
                -m, --message <MESSAGE>\n      \
                --conflict <CONFLICT>...  Repeatable\n  \
                -C <DIRECTORY>\n\n\
                Folder arguments take --label too.\n";
    let commands: Vec<String> = commands_of(help).into_iter().collect();
    assert_eq!(commands, vec!["configure-mount", "status"]);
    let specs = options_of(help);
    let names: Vec<Vec<&str>> = specs
        .iter()
        .map(|spec| spec.names.iter().map(String::as_str).collect())
        .collect();
    assert_eq!(
        names,
        vec![
            vec!["--json"],
            vec!["-m", "--message"],
            vec!["--conflict"],
            vec!["-C"],
        ]
    );
    let takes: Vec<Takes> = specs.iter().map(|spec| spec.takes).collect();
    assert_eq!(
        takes,
        vec![Takes::Nothing, Takes::One, Takes::Many, Takes::One]
    );
    // Names in descriptions or after the section are not options, and a
    // prefix of a name is not that name.
    for absent in ["--mount", "-x", "--wrapped", "--label", "--mess"] {
        assert!(find_option(&specs, absent).is_none(), "{absent}");
    }
}

#[test]
fn the_usage_parser_counts_positionals_and_required_options() {
    assert_eq!(
        usage_of("Usage: tkfs.exe manage receive [OPTIONS] <ENDPOINT> <REPO> <LABEL>\n"),
        Usage {
            required: 3,
            ..Usage::default()
        }
    );
    assert_eq!(
        usage_of(
            "Recover\n\nUsage: tkfs recover [OPTIONS] --conflict <CONFLICT>... <ENTITY> <TARGET>\n"
        ),
        Usage {
            required: 2,
            required_options: vec!["--conflict".to_owned()],
            ..Usage::default()
        }
    );
    // A required flag: the next word is a positional argument, as the
    // `Options:` section tells; a required option with a value takes it.
    assert_eq!(
        usage_of(
            "Usage: tkfs publish [OPTIONS] --current-state-only <NAME>\n\n\
             Arguments:\n  <NAME>  \n\n\
             Options:\n      --current-state-only  \n"
        ),
        Usage {
            required: 1,
            required_options: vec!["--current-state-only".to_owned()],
            ..Usage::default()
        }
    );
    assert_eq!(
        usage_of(
            "Usage: tkfs daemon [OPTIONS] --state <STATE>\n\n\
             Options:\n      --state <STATE>  \n"
        ),
        Usage {
            required_options: vec!["--state".to_owned()],
            ..Usage::default()
        }
    );
    assert_eq!(
        usage_of("Usage: tkfs x [OPTIONS] <A> [B] [C]...\n"),
        Usage {
            required: 1,
            optional: 2,
            variadic: true,
            required_options: Vec::new(),
        }
    );
    assert_eq!(
        usage_of("Usage: tkfs.exe manage [OPTIONS] <COMMAND>\n"),
        Usage::default()
    );
}

#[test]
fn the_checker_refuses_unknown_commands_options_and_incomplete_lines() {
    let mut helper = Helper::new();
    // Real commands pass: global options before or after the command, a
    // hidden alias, placeholders, option values (one word, quoted words, and
    // every word up to the next option for a multi-value option), required
    // options by their long or short name, and a help request.
    for good in [
        "tkfs -f orchestrator.toml manage stop STATE --no-wait",
        "tkfs manage status --json",
        "tkfs -C MOUNT checkpoint -m MESSAGE",
        "tkfs -C MOUNT checkpoint --message \"two words\"",
        "tkfs --runtime STATE/runtime.json status",
        "tkfs start",
        "tkfs manage receive wss://h REPO_ID LABEL --mount D:\\Work\\Projects",
        "tkfs manage revalidate STATE --id A --id B",
        "tkfs -C MOUNT recover ENTITY TARGET --conflict A B",
        "tkfs -C MOUNT resolve --conflict A --revision R",
        "tkfs daemon --state DIR --mount PATH",
        "tkfs manage stop --help",
    ] {
        let checked = check(&mut helper, &line(good));
        assert!(checked.is_ok(), "{good}: {checked:?}");
    }
    assert_eq!(
        check(&mut helper, &line("tkfs manage stop STATE")).unwrap(),
        vec!["manage".to_owned(), "stop".to_owned()]
    );
    // Refused: a command that does not exist, an option a command does not
    // have, a command group without its subcommand, a missing or an extra
    // positional argument (also one swallowed by a multi-value option), and
    // a missing required option.
    for (bad, why) in [
        ("tkfs manage bogus STATE", "is not a subcommand"),
        ("tkfs bogus", "is not a subcommand"),
        ("tkfs manage stop STATE --bogus", "does not list --bogus"),
        ("tkfs manage pause STATE --force", "does not list --force"),
        (
            "tkfs checkpoint --message-text MESSAGE",
            "does not list --message-text",
        ),
        ("tkfs manage", "needs a subcommand"),
        ("tkfs manage receive wss://h REPO_ID", "needs 3 positional"),
        ("tkfs manage stop", "needs 1 positional"),
        ("tkfs manage stop STATE OTHER", "takes at most 1 positional"),
        ("tkfs manage list STATE", "takes at most 0 positional"),
        (
            "tkfs -C MOUNT recover --conflict A ENTITY TARGET",
            "needs 2 positional",
        ),
        ("tkfs daemon --mount PATH", "requires --state"),
        ("tkfs -C MOUNT checkpoint", "requires --message"),
        (
            "tkfs -C MOUNT publish NAME",
            "requires --current-state-only",
        ),
    ] {
        match check(&mut helper, &line(bad)) {
            Ok(path) => panic!("{bad}: accepted as {path:?}"),
            Err(error) => assert!(error.contains(why), "{bad}: {error}"),
        }
    }
}
