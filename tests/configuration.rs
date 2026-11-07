//! Application configuration uses explicit inputs without mutating process environment.

#[path = "../examples/configured_app/settings.rs"]
mod settings;

use clap::Parser;
use settings::{Cli, load};
use std::{
    collections::HashMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn environment(values: &[(&str, &str)]) -> HashMap<String, String> {
    settings::environment(
        values
            .iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
    )
    .unwrap()
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "configuration-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn toml(&self, contents: &str) -> PathBuf {
        let path = self.0.join("settings.toml");
        fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn cli(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("configured_app").chain(args.iter().copied())).unwrap()
}

#[test]
fn defaults_are_explicit_when_no_file_was_selected() {
    let loaded = load(&cli(&[]), HashMap::new()).unwrap();
    assert_eq!(loaded.settings.interval(), Duration::from_secs(1));
    assert_eq!(
        loaded.settings.output_file(),
        Path::new("configured-samples.log")
    );
    assert_eq!(
        loaded.explain(),
        "sample_interval_ms = 1000 [default]\noutput_file = \"configured-samples.log\" [default]"
    );
}

#[test]
fn file_environment_and_cli_override_only_their_fields() {
    let directory = Directory::new();
    let path = directory.toml("sample_interval_ms = 200\noutput_file = 'file samples.log'\n");
    let mut options = cli(&[]);
    options.config = Some(path.clone());
    let loaded = load(&options, HashMap::new()).unwrap();
    assert_eq!(loaded.settings.interval(), Duration::from_millis(200));
    assert_eq!(loaded.settings.output_file(), Path::new("file samples.log"));
    assert!(loaded.explain().contains(path.to_str().unwrap()));

    let env = environment(&[("SAMPLER_SAMPLE_INTERVAL_MS", "300")]);
    let loaded = load(&options, env).unwrap();
    assert_eq!(loaded.settings.interval(), Duration::from_millis(300));
    assert_eq!(loaded.settings.output_file(), Path::new("file samples.log"));
    assert!(
        loaded
            .explain()
            .lines()
            .next()
            .unwrap()
            .contains("the environment")
    );
    assert!(
        loaded
            .explain()
            .lines()
            .nth(1)
            .unwrap()
            .contains(path.to_str().unwrap())
    );

    options.interval_ms = Some(400);
    options.output = Some("cli samples.log".into());
    let loaded = load(
        &options,
        environment(&[
            ("SAMPLER_SAMPLE_INTERVAL_MS", "300"),
            ("SAMPLER_OUTPUT_FILE", "env.log"),
        ]),
    )
    .unwrap();
    assert_eq!(loaded.settings.interval(), Duration::from_millis(400));
    assert_eq!(loaded.settings.output_file(), Path::new("cli samples.log"));
    assert!(loaded.explain().contains("CLI --interval-ms"));
    assert!(loaded.explain().contains("CLI --output"));
}

#[test]
fn environment_literal_paths_are_not_parsed_as_numbers_or_booleans() {
    for path in ["123", "true", "file with spaces.log", "  spaced.log  "] {
        let loaded = load(&cli(&[]), environment(&[("SAMPLER_OUTPUT_FILE", path)])).unwrap();
        assert_eq!(loaded.settings.output_file(), Path::new(path));
    }
}

#[test]
fn explanation_does_not_disclose_unrelated_environment_values() {
    let loaded = load(
        &cli(&["--explain"]),
        environment(&[("UNRELATED_SECRET", "do-not-print-this")]),
    )
    .unwrap();
    assert!(!loaded.explain().contains("do-not-print-this"));
    assert_eq!(loaded.explain().lines().count(), 2);
    assert!(cli(&["--explain"]).explain);
}

#[test]
fn environment_aliases_are_rejected_independently_of_values_order_or_cli_override() {
    for (upper, lower) in [
        ("same-private-value", "same-private-value"),
        ("private-upper", "private-lower"),
    ] {
        for reversed in [false, true] {
            let mut pairs = [
                ("SAMPLER_OUTPUT_FILE", upper),
                ("sampler_output_file", lower),
            ];
            if reversed {
                pairs.reverse();
            }
            let direct = pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect::<HashMap<_, _>>();
            let collected = settings::environment(
                pairs
                    .iter()
                    .map(|(name, value)| (OsString::from(name), OsString::from(value))),
            )
            .unwrap();
            for variables in [direct, collected] {
                for args in [&[][..], &["--output", "CLI-wins-only-valid-inputs"][..]] {
                    let error = load(&cli(args), variables.clone()).unwrap_err();
                    assert!(matches!(
                        &error,
                        settings::SettingsError::EnvironmentAlias { .. }
                    ));
                    let message = error.to_string();
                    assert!(message.contains("SAMPLER_OUTPUT_FILE"));
                    assert!(message.contains("sampler_output_file"));
                    assert!(!message.contains(upper));
                    assert!(!message.contains(lower));
                    assert_eq!(
                        message,
                        "environment variables \"SAMPLER_OUTPUT_FILE\" and \"sampler_output_file\" refer to the same setting"
                    );
                }
            }
        }
    }
    // The application's namespace does not impose rules on other environments.
    let unrelated = HashMap::from([
        (String::from("HOME"), String::from("upper")),
        (String::from("home"), String::from("lower")),
    ]);
    assert!(load(&cli(&[]), unrelated).is_ok());
}

#[cfg(any(unix, windows))]
#[test]
fn only_selected_environment_requires_unicode_and_errors_do_not_show_values() {
    fn non_unicode() -> OsString {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![0xff])
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0xd800])
        }
    }
    let selected = settings::environment([
        (OsString::from("UNRELATED_SECRET"), non_unicode()),
        (non_unicode(), OsString::from("unrelated")),
        (
            OsString::from("sampler_sample_interval_ms"),
            OsString::from("20"),
        ),
    ])
    .unwrap();
    assert_eq!(
        load(&cli(&[]), selected).unwrap().settings.interval(),
        Duration::from_millis(20)
    );
    let error = settings::environment([(OsString::from("SAMPLER_OUTPUT_FILE"), non_unicode())])
        .unwrap_err();
    assert!(error.to_string().contains("SAMPLER_OUTPUT_FILE"));
    let mut invalid_name = OsString::from("SAMPLER_");
    invalid_name.push(non_unicode());
    let error =
        settings::environment([(invalid_name, OsString::from("do-not-print-this"))]).unwrap_err();
    assert!(!error.to_string().contains("do-not-print-this"));
}

#[test]
fn explicitly_selected_missing_or_invalid_file_fails() {
    let directory = Directory::new();
    let mut options = cli(&[]);
    options.config = Some(directory.0.join("missing.toml"));
    assert!(load(&options, HashMap::new()).is_err());
    // An explicitly selected missing name must not silently resolve a sibling.
    fs::write(
        directory.0.join("missing.toml.toml"),
        "sample_interval_ms = 2",
    )
    .unwrap();
    assert!(load(&options, HashMap::new()).is_err());
    options.config = Some(directory.toml("sample_interval_ms = [broken"));
    assert!(load(&options, HashMap::new()).is_err());
    fs::write(options.config.as_ref().unwrap(), [0xff, 0xfe]).unwrap();
    assert!(load(&options, HashMap::new()).is_err());
}

#[test]
fn unknown_file_fields_and_prefixed_environment_keys_fail() {
    let directory = Directory::new();
    let mut options = cli(&[]);
    options.config = Some(directory.toml("sample_interval_ms = 1\nunknown = 'ignored?'"));
    assert!(
        load(&options, HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("unknown field")
    );
    assert!(
        load(&cli(&[]), environment(&[("SAMPLER_UNKNOWN", "value")]))
            .unwrap_err()
            .to_string()
            .contains("unknown field")
    );
}

#[test]
fn invalid_intervals_and_paths_fail_at_configuration_boundary() {
    for interval in ["0", "-1", "", "true", "yes", "1.5", "18446744073709551616"] {
        assert!(
            load(
                &cli(&[]),
                environment(&[("SAMPLER_SAMPLE_INTERVAL_MS", interval)])
            )
            .is_err(),
            "accepted invalid interval {interval:?}"
        );
    }
    for path in ["", " ", "\t"] {
        assert!(load(&cli(&[]), environment(&[("SAMPLER_OUTPUT_FILE", path)])).is_err());
    }
    assert!(load(&cli(&["--interval-ms", "0"]), HashMap::new()).is_err());
    assert!(load(&cli(&["--output", ""]), HashMap::new()).is_err());
}

#[test]
fn toml_types_do_not_silently_round_or_coerce_into_settings() {
    let directory = Directory::new();
    for contents in [
        "sample_interval_ms = 1.5",
        "sample_interval_ms = true",
        "sample_interval_ms = -1",
        "output_file = 123",
        "output_file = false",
    ] {
        let mut options = cli(&[]);
        options.config = Some(directory.toml(contents));
        assert!(
            load(&options, HashMap::new()).is_err(),
            "accepted wrong TOML type: {contents}"
        );
    }
}

#[test]
fn invalid_cli_and_help_are_handled_before_configuration_loading() {
    for args in [
        &["--unknown"][..],
        &["--interval-ms", "-1"],
        &["--interval-ms", "no"],
        &["--interval-ms", "18446744073709551616"],
        &["--config"],
    ] {
        assert!(
            Cli::try_parse_from(std::iter::once("configured_app").chain(args.iter().copied()))
                .is_err()
        );
    }
    assert_eq!(
        Cli::try_parse_from(["configured_app", "--help"])
            .unwrap_err()
            .kind(),
        clap::error::ErrorKind::DisplayHelp
    );
}

#[test]
fn partially_specified_file_keeps_unmodified_default_origin() {
    let directory = Directory::new();
    let mut options = cli(&[]);
    options.config = Some(directory.toml("sample_interval_ms = 25"));
    let loaded = load(&options, HashMap::new()).unwrap();
    assert_eq!(loaded.settings.interval(), Duration::from_millis(25));
    assert!(
        loaded
            .explain()
            .lines()
            .nth(1)
            .unwrap()
            .ends_with("[default]")
    );
}
