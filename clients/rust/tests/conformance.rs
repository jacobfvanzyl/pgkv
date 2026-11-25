//! Differential test of the mapping layer: every scenario in `conformance/scenarios.jsonl` is
//! replayed through one generic function over `redis::aio::ConnectionLike`, once against a real
//! Redis (redis-rs's own connection, RESP2) and once against pgkv, and the `redis::Value`s and
//! errors are compared command by command. A second test replays every scenario as a single
//! MULTI/EXEC pipeline and compares the replies slot by slot.
//!
//! The SQL semantics are verified by `conformance/harness.py`; the comparison rules here are
//! the same as its `equal()` (unordered replies, random commands, clock-dependent replies,
//! opaque cursors, INCRBYFLOAT digits), and `conformance/expected.json` lists the documented
//! differences. Errors are compared more strictly than there: kind, code and detail.

mod common;

use std::path::Path;
use std::time::Duration;

use pgkv::Connection;
use redis::aio::ConnectionLike;
use redis::{RedisError, RedisResult, Value};

/// A reply in a form that can be sorted and printed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Reply {
    Nil,
    Int(i64),
    Bulk(Vec<u8>),
    Status(String),
    Array(Vec<Reply>),
    /// A value RESP2 commands should never produce here.
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ErrorReply {
    kind: String,
    code: Option<String>,
    detail: Option<String>,
}

type Outcome = Result<Reply, ErrorReply>;

fn reply(value: Value) -> Reply {
    match value {
        Value::Nil => Reply::Nil,
        Value::Int(n) => Reply::Int(n),
        Value::BulkString(bytes) => Reply::Bulk(bytes),
        Value::Okay => Reply::Status("OK".into()),
        Value::SimpleString(s) => Reply::Status(s),
        Value::Array(items) => Reply::Array(items.into_iter().map(reply).collect()),
        other => Reply::Other(format!("{other:?}")),
    }
}

fn error_reply(error: &RedisError) -> ErrorReply {
    ErrorReply {
        kind: format!("{:?}", error.kind()),
        code: error.code().map(str::to_owned),
        // A failure that is not a Redis error reply has no detail; keep its text instead.
        detail: match error.code() {
            Some(_) => error.detail().map(str::to_owned),
            None => Some(error.to_string()),
        },
    }
}

/// The one code path both servers go through.
async fn run<C: ConnectionLike>(con: &mut C, argv: &[String]) -> Outcome {
    let mut cmd = redis::cmd(&argv[0]);
    for word in &argv[1..] {
        cmd.arg(word);
    }
    match cmd.query_async::<Value>(con).await {
        Ok(value) => Ok(reply(value)),
        Err(error) => Err(error_reply(&error)),
    }
}

const UNORDERED: &[&str] = &[
    "KEYS", "HKEYS", "HVALS", "SMEMBERS", "SINTER", "SUNION", "SDIFF",
];
const RANDOM: &[&str] = &[
    "SPOP",
    "SRANDMEMBER",
    "HRANDFIELD",
    "ZRANDMEMBER",
    "RANDOMKEY",
];
const LEX: &[&str] = &[
    "ZRANGEBYLEX",
    "ZREVRANGEBYLEX",
    "ZLEXCOUNT",
    "ZREMRANGEBYLEX",
    "ZRANGE",
    "ZRANGESTORE",
];

fn clock_tolerance(name: &str) -> Option<i64> {
    match name {
        "TTL" | "EXPIRETIME" => Some(1),
        "PTTL" | "PEXPIRETIME" => Some(1500),
        _ => None,
    }
}

fn has_word(argv: &[String], word: &str) -> bool {
    argv.iter().any(|a| a.eq_ignore_ascii_case(word))
}

fn is_decimal(bytes: &[u8]) -> bool {
    let digits = bytes.strip_prefix(b"-").unwrap_or(bytes);
    let mut parts = digits.splitn(2, |&b| b == b'.');
    let all_digits = |part: &[u8]| !part.is_empty() && part.iter().all(u8::is_ascii_digit);
    parts.next().is_some_and(all_digits) && parts.next().is_none_or(all_digits)
}

/// Equality, except that two long decimal strings standing for the same number to 15 significant
/// digits are the same (INCRBYFLOAT is long double arithmetic in Redis, decimal in pgkv).
fn same(a: &Reply, b: &Reply) -> bool {
    match (a, b) {
        (Reply::Array(x), Reply::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y))
        }
        (Reply::Bulk(x), Reply::Bulk(y)) if x != y => {
            if x.len().max(y.len()) < 15 || !is_decimal(x) || !is_decimal(y) {
                return false;
            }
            let parse = |bytes: &[u8]| String::from_utf8_lossy(bytes).parse::<f64>();
            let (Ok(fa), Ok(fb)) = (parse(x), parse(y)) else {
                return false;
            };
            fa == fb
                || (fa - fb).abs() <= 2e-17
                || (fa != 0.0 && fa.is_finite() && ((fa - fb) / fa).abs() < 1e-15)
        }
        _ => a == b,
    }
}

fn same_unordered(a: &[Reply], b: &[Reply]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut rest: Vec<&Reply> = b.iter().collect();
    a.iter()
        .all(|x| match rest.iter().position(|y| same(x, y)) {
            Some(i) => {
                rest.swap_remove(i);
                true
            }
            None => false,
        })
}

fn pairs(items: &[Reply]) -> Vec<Reply> {
    items
        .chunks(2)
        .map(|pair| Reply::Array(pair.to_vec()))
        .collect()
}

fn same_error(a: &ErrorReply, b: &ErrorReply) -> bool {
    let lower = |detail: &Option<String>| detail.as_ref().map(|d| d.trim().to_lowercase());
    a.kind == b.kind && a.code == b.code && lower(&a.detail) == lower(&b.detail)
}

/// Whether `a` (Redis) and `b` (pgkv) are the same answer to `argv`.
fn equal(argv: &[String], a: &Outcome, b: &Outcome, unspecified: bool) -> bool {
    let name = argv[0].to_uppercase();
    let (a, b) = match (a, b) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(a), Err(b)) => return same_error(a, b),
        _ => return false,
    };
    if name == "TIME" {
        return true;
    }
    if RANDOM.contains(&name.as_str()) {
        return match (a, b) {
            (Reply::Array(x), Reply::Array(y)) => x.len() == y.len(),
            _ => std::mem::discriminant(a) == std::mem::discriminant(b),
        };
    }
    if let Some(tolerance) = clock_tolerance(&name) {
        return match (a, b) {
            (Reply::Int(x), Reply::Int(y)) if *x >= 0 && *y >= 0 => (x - y).abs() <= tolerance,
            _ => a == b,
        };
    }
    if matches!(name.as_str(), "SCAN" | "HSCAN" | "SSCAN" | "ZSCAN") {
        // Cursors are opaque. Compare the elements when both finished in one call.
        let (Reply::Array(x), Reply::Array(y)) = (a, b) else {
            return a == b;
        };
        let ([xc, Reply::Array(xs)], [yc, Reply::Array(ys)]) = (x.as_slice(), y.as_slice()) else {
            return a == b;
        };
        let done = Reply::Bulk(b"0".to_vec());
        if *xc != done || *yc != done {
            return true;
        }
        let paired = matches!(name.as_str(), "HSCAN" | "ZSCAN") && !has_word(argv, "NOVALUES");
        return if paired {
            same_unordered(&pairs(xs), &pairs(ys))
        } else {
            same_unordered(xs, ys)
        };
    }
    if unspecified {
        return true;
    }
    if let (Reply::Array(x), Reply::Array(y)) = (a, b) {
        if UNORDERED.contains(&name.as_str()) {
            return same_unordered(x, y);
        }
        if name == "HGETALL" {
            return same_unordered(&pairs(x), &pairs(y));
        }
    }
    same(a, b)
}

/// Lexicographic range commands are only defined for sorted sets whose scores are all equal.
async fn lex_over_mixed_scores<C: ConnectionLike>(oracle: &mut C, argv: &[String]) -> bool {
    let name = argv[0].to_uppercase();
    if !LEX.contains(&name.as_str()) || argv.len() < 3 {
        return false;
    }
    if matches!(name.as_str(), "ZRANGE" | "ZRANGESTORE") && !has_word(argv, "BYLEX") {
        return false;
    }
    let key = if name == "ZRANGESTORE" {
        &argv[2]
    } else {
        &argv[1]
    };
    let dump = ["ZRANGE", key, "0", "-1", "WITHSCORES"].map(String::from);
    match run(oracle, &dump).await {
        Ok(Reply::Array(items)) => {
            let mut scores: Vec<&Reply> = items.iter().skip(1).step_by(2).collect();
            scores.sort();
            scores.dedup();
            scores.len() > 1
        }
        _ => false,
    }
}

/// The reply as `conformance/harness.py` prints it, as far as `expected.json` looks at it.
fn harness_repr(outcome: &Outcome) -> String {
    match outcome {
        Err(error) => {
            let detail = error.detail.as_deref().unwrap_or_default().trim();
            match error.code.as_deref() {
                // Like redis-py, the harness drops the generic "ERR " code.
                Some("ERR") | None => format!("Err('{detail}')"),
                Some(code) => format!("Err('{code} {detail}')"),
            }
        }
        Ok(reply) => format!("{reply:?}"),
    }
}

struct Expected {
    command: String,
    pattern: regex::Regex,
    why: String,
}

fn load_expected(path: &Path) -> Vec<Expected> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let entries: serde_json::Value = serde_json::from_str(&text).expect("expected.json");
    entries
        .as_array()
        .expect("expected.json is an array")
        .iter()
        .map(|entry| Expected {
            command: entry["command"].as_str().unwrap_or("*").to_owned(),
            pattern: regex::Regex::new(entry["match"].as_str().expect("match")).expect("regex"),
            why: entry["why"].as_str().expect("why").to_owned(),
        })
        .collect()
}

struct Scenario {
    name: String,
    commands: Vec<Vec<String>>,
}

fn load_scenarios(path: &Path) -> Vec<Scenario> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let json: serde_json::Value = serde_json::from_str(line).expect("scenario line");
            Scenario {
                name: json["name"].as_str().expect("name").to_owned(),
                commands: json["cmds"]
                    .as_array()
                    .expect("cmds")
                    .iter()
                    .map(|cmd| {
                        cmd.as_array()
                            .expect("command")
                            .iter()
                            .map(|word| word.as_str().expect("word").to_owned())
                            .collect()
                    })
                    .collect(),
            }
        })
        .collect()
}

/// Both tests flush the two databases between scenarios, so they take turns.
static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

enum Stop {
    /// Every command matched (or the scenario reached a point where the data may differ).
    Matched,
    Documented(String),
    Diverged(String),
}

async fn run_scenario<O: ConnectionLike, P: ConnectionLike>(
    oracle: &mut O,
    pgkv: &mut P,
    scenario: &Scenario,
    expected: &[Expected],
    ran: &mut usize,
) -> Stop {
    let flush = ["FLUSHDB".to_owned()];
    assert_eq!(run(oracle, &flush).await, Ok(Reply::Status("OK".into())));
    assert_eq!(run(pgkv, &flush).await, Ok(Reply::Status("OK".into())));

    for (step, original) in scenario.commands.iter().enumerate() {
        let name = original[0].to_uppercase();
        if name == "SLEEP_MS" {
            let ms = original[1].parse().expect("SLEEP_MS milliseconds");
            tokio::time::sleep(Duration::from_millis(ms)).await;
            continue;
        }
        // The oracle server is shared: only ever flush the database this test selected.
        let argv = if name == "FLUSHALL" {
            &flush[..]
        } else {
            &original[..]
        };

        let unspecified = lex_over_mixed_scores(oracle, argv).await;
        let a = run(oracle, argv).await;
        let b = run(pgkv, argv).await;
        *ran += 1;

        if !equal(argv, &a, &b, unspecified) {
            let text = format!(
                "oracle={} pgkv={} state=None",
                harness_repr(&a),
                harness_repr(&b)
            );
            let documented = expected
                .iter()
                .find(|e| (e.command == "*" || e.command == name) && e.pattern.is_match(&text));
            return match documented {
                Some(e) => Stop::Documented(e.why.clone()),
                None => Stop::Diverged(format!(
                    "{} step {step}: {argv:?}\n    redis: {a:?}\n    pgkv:  {b:?}",
                    scenario.name
                )),
            };
        }
        if a.is_ok() && (unspecified || RANDOM.contains(&name.as_str())) {
            return Stop::Matched;
        }
        if matches!(name.as_str(), "INCRBYFLOAT" | "HINCRBYFLOAT") && a != b {
            // Same number, different digits: the stored strings now differ.
            return Stop::Matched;
        }
    }
    Stop::Matched
}

#[tokio::test]
async fn scenarios_give_the_same_values_through_redis_rs() {
    let (Some(mut client), Some(mut oracle)) = (common::pg().await, common::oracle().await) else {
        return;
    };
    let _exclusive = EXCLUSIVE.lock().await;
    let mut pgkv = Connection::new(&mut client);

    let conformance = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance");
    let scenarios = load_scenarios(&conformance.join("scenarios.jsonl"));
    let expected = load_expected(&conformance.join("expected.json"));

    let (mut ran, mut matched) = (0, 0);
    let mut documented = std::collections::BTreeMap::<String, usize>::new();
    let mut diverged = Vec::new();
    for scenario in &scenarios {
        match run_scenario(&mut oracle, &mut pgkv, scenario, &expected, &mut ran).await {
            Stop::Matched => matched += 1,
            Stop::Documented(why) => *documented.entry(why).or_default() += 1,
            Stop::Diverged(report) => diverged.push(report),
        }
    }
    let flush = ["FLUSHDB".to_owned()];
    let _ = run(&mut oracle, &flush).await;
    let _ = run(&mut pgkv, &flush).await;

    println!(
        "{} scenarios, {ran} commands: {matched} match, {} stop at a documented difference, {} diverge",
        scenarios.len(),
        documented.values().sum::<usize>(),
        diverged.len()
    );
    for (why, count) in &documented {
        println!("  documented ({count}): {why}");
    }
    for report in &diverged {
        println!("\n{report}");
    }
    assert!(ran > 0, "no commands were replayed");
    assert!(diverged.is_empty(), "{} scenarios diverge", diverged.len());
}

/// What `.atomic().ignore_errors()` gives for the commands: a result per command, or the error
/// of a transaction that was discarded (its kind and code; the text differs, see the README).
async fn run_transaction<C: ConnectionLike>(
    con: &mut C,
    commands: &[&[String]],
) -> Result<Vec<Outcome>, String> {
    let mut pipe = redis::pipe();
    pipe.atomic().ignore_errors();
    for argv in commands {
        let cmd = pipe.cmd(&argv[0]);
        for word in &argv[1..] {
            cmd.arg(word);
        }
    }
    match pipe.query_async::<Vec<RedisResult<Value>>>(con).await {
        Ok(results) => Ok(results
            .into_iter()
            .map(|result| result.map(reply).map_err(|e| error_reply(&e)))
            .collect()),
        Err(error) => Err(format!("{:?} {:?}", error.kind(), error.code())),
    }
}

/// Replays the scenario as one transaction on both servers. `None` if it cannot be one (it
/// sleeps between commands).
async fn run_scenario_as_transaction<O: ConnectionLike, P: ConnectionLike>(
    oracle: &mut O,
    pgkv: &mut P,
    scenario: &Scenario,
    expected: &[Expected],
    ran: &mut usize,
) -> Option<Stop> {
    if scenario
        .commands
        .iter()
        .any(|c| c[0].eq_ignore_ascii_case("SLEEP_MS"))
    {
        return None;
    }
    let flush = ["FLUSHDB".to_owned()];
    // The oracle server is shared: only ever flush the database this test selected.
    let commands: Vec<&[String]> = scenario
        .commands
        .iter()
        .map(|argv| match argv[0].eq_ignore_ascii_case("FLUSHALL") {
            true => &flush[..],
            false => &argv[..],
        })
        .collect();

    // Which replies are unspecified depends on the data at that step, which cannot be looked
    // at in the middle of a transaction: find out with a run of single commands first.
    assert_eq!(run(oracle, &flush).await, Ok(Reply::Status("OK".into())));
    let mut unspecified = Vec::with_capacity(commands.len());
    for argv in &commands {
        unspecified.push(lex_over_mixed_scores(oracle, argv).await);
        let _ = run(oracle, argv).await;
    }
    assert_eq!(run(oracle, &flush).await, Ok(Reply::Status("OK".into())));
    assert_eq!(run(pgkv, &flush).await, Ok(Reply::Status("OK".into())));

    let theirs = run_transaction(oracle, &commands).await;
    let ours = run_transaction(pgkv, &commands).await;
    *ran += commands.len();
    let (theirs, ours) = match (theirs, ours) {
        (Ok(theirs), Ok(ours)) => (theirs, ours),
        // Discarded by both: the same error, and neither may have applied anything.
        (Err(theirs), Err(ours)) => {
            let empty = ["DBSIZE".to_owned()];
            let sizes = (run(oracle, &empty).await, run(pgkv, &empty).await);
            return Some(
                if theirs == ours && sizes == (Ok(Reply::Int(0)), Ok(Reply::Int(0))) {
                    Stop::Matched
                } else {
                    Stop::Diverged(format!(
                        "{}: discarded\n    redis: {theirs}\n    pgkv:  {ours}\n    sizes: {sizes:?}",
                        scenario.name
                    ))
                },
            );
        }
        (theirs, ours) => {
            return Some(Stop::Diverged(format!(
                "{}: one transaction was discarded\n    redis: {theirs:?}\n    pgkv:  {ours:?}",
                scenario.name
            )));
        }
    };
    assert_eq!((theirs.len(), ours.len()), (commands.len(), commands.len()));

    for (step, argv) in commands.iter().enumerate() {
        let name = argv[0].to_uppercase();
        let (a, b) = (&theirs[step], &ours[step]);
        if !equal(argv, a, b, unspecified[step]) {
            let text = format!(
                "oracle={} pgkv={} state=None",
                harness_repr(a),
                harness_repr(b)
            );
            let documented = expected
                .iter()
                .find(|e| (e.command == "*" || e.command == name) && e.pattern.is_match(&text));
            return Some(match documented {
                Some(e) => Stop::Documented(e.why.clone()),
                None => Stop::Diverged(format!(
                    "{} step {step}: {argv:?}\n    redis: {a:?}\n    pgkv:  {b:?}",
                    scenario.name
                )),
            });
        }
        // From here on the data may differ, as in the command-by-command replay.
        if a.is_ok() && (unspecified[step] || RANDOM.contains(&name.as_str())) {
            break;
        }
        if matches!(name.as_str(), "INCRBYFLOAT" | "HINCRBYFLOAT") && a != b {
            break;
        }
    }
    Some(Stop::Matched)
}

#[tokio::test]
async fn scenarios_give_the_same_values_as_one_transaction() {
    let (Some(mut client), Some(mut oracle)) = (common::pg().await, common::oracle().await) else {
        return;
    };
    // The other test in this file flushes the same two databases.
    let _exclusive = EXCLUSIVE.lock().await;
    let mut pgkv = Connection::new(&mut client);

    let conformance = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance");
    let scenarios = load_scenarios(&conformance.join("scenarios.jsonl"));
    let expected = load_expected(&conformance.join("expected.json"));

    let (mut ran, mut matched, mut skipped) = (0, 0, 0);
    let mut documented = std::collections::BTreeMap::<String, usize>::new();
    let mut diverged = Vec::new();
    for scenario in &scenarios {
        let stop =
            run_scenario_as_transaction(&mut oracle, &mut pgkv, scenario, &expected, &mut ran);
        match stop.await {
            None => skipped += 1,
            Some(Stop::Matched) => matched += 1,
            Some(Stop::Documented(why)) => *documented.entry(why).or_default() += 1,
            Some(Stop::Diverged(report)) => diverged.push(report),
        }
    }
    let flush = ["FLUSHDB".to_owned()];
    let _ = run(&mut oracle, &flush).await;
    let _ = run(&mut pgkv, &flush).await;

    println!(
        "{} scenarios as one MULTI/EXEC each, {ran} commands: {matched} match, {} stop at a documented difference, {} diverge, {skipped} skipped (they sleep)",
        scenarios.len() - skipped,
        documented.values().sum::<usize>(),
        diverged.len()
    );
    for (why, count) in &documented {
        println!("  documented ({count}): {why}");
    }
    for report in &diverged {
        println!("\n{report}");
    }
    assert!(ran > 0, "no commands were replayed");
    assert!(diverged.is_empty(), "{} scenarios diverge", diverged.len());
}
