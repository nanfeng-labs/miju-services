use clap::Parser;
use config::{Config, Environment, FileFormat, Format, Source, Value, ValueKind};
use serde::Deserialize;
use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Application options; no configuration convention is imposed on the library.
#[derive(Debug, Parser)]
#[command(about = "Record local samples with application-owned configuration and resources")]
pub(crate) struct Cli {
    /// Read this TOML file; no file is searched for when omitted.
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    /// Override the sample interval in milliseconds.
    #[arg(long)]
    pub(crate) interval_ms: Option<u64>,
    /// Override the output file, relative to the application's working directory.
    #[arg(long)]
    pub(crate) output: Option<String>,
    /// Print the two effective settings and their sources, then exit without opening resources.
    #[arg(long)]
    pub(crate) explain: bool,
}

/// Validated application settings passed directly to the sampling service.
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    interval: Duration,
    output_file: PathBuf,
}

impl Settings {
    pub(crate) fn new(
        sample_interval_ms: u64,
        output_file: PathBuf,
    ) -> Result<Self, SettingsError> {
        let interval = Duration::from_millis(sample_interval_ms);
        if interval.is_zero() || Instant::now().checked_add(interval).is_none() {
            return Err(SettingsError::InvalidInterval);
        }
        if output_file.as_os_str().to_string_lossy().trim().is_empty() {
            return Err(SettingsError::EmptyOutput);
        }
        Ok(Self {
            interval,
            output_file,
        })
    }

    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    pub(crate) fn output_file(&self) -> &Path {
        &self.output_file
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSettings {
    sample_interval_ms: u64,
    output_file: PathBuf,
}

/// The values and origins of the same merged configuration snapshot.
#[derive(Debug)]
pub(crate) struct LoadedSettings {
    pub(crate) settings: Settings,
    interval_origin: String,
    output_origin: String,
}

impl LoadedSettings {
    pub(crate) fn explain(&self) -> String {
        format!(
            "sample_interval_ms = {} [{}]\noutput_file = {:?} [{}]",
            self.settings.interval().as_millis(),
            self.interval_origin,
            self.settings.output_file(),
            self.output_origin,
        )
    }
}

/// Loading errors remain in the application layer, before resources are opened.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SettingsError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error("environment variable {0:?} must use Unicode names and values")]
    Environment(OsString),
    #[error("environment variables {first:?} and {second:?} refer to the same setting")]
    EnvironmentAlias { first: String, second: String },
    #[error("cannot read configuration file {path:?}: {source}")]
    File {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("sample_interval_ms must be a positive, representable integer number of milliseconds")]
    InvalidInterval,
    #[error("output_file must be a nonempty path string")]
    EmptyOutput,
}

pub(crate) fn environment(
    variables: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<HashMap<String, String>, SettingsError> {
    let mut selected = HashMap::new();
    for (name, value) in variables {
        if !name
            .as_encoded_bytes()
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"SAMPLER_"))
        {
            continue;
        }
        let name = name.into_string().map_err(SettingsError::Environment)?;
        let value = value
            .into_string()
            .map_err(|_| SettingsError::Environment(OsString::from(&name)))?;
        selected.insert(name, value);
    }
    Ok(selected)
}

pub(crate) fn load(
    cli: &Cli,
    environment: HashMap<String, String>,
) -> Result<LoadedSettings, SettingsError> {
    // config lowercases environment names; reject ambiguous aliases before merging.
    let mut names = environment
        .keys()
        .filter_map(|name| {
            let canonical = name.to_lowercase();
            canonical
                .starts_with("sampler_")
                .then_some((canonical, name))
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    if let Some(pair) = names.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(SettingsError::EnvironmentAlias {
            first: pair[0].1.to_owned(),
            second: pair[1].1.to_owned(),
        });
    }
    let default = String::from("default");
    let mut builder = Config::builder()
        .set_default("sample_interval_ms", Value::new(Some(&default), 1000_u64))?
        .set_default(
            "output_file",
            Value::new(Some(&default), "configured-samples.log"),
        )?;
    if let Some(path) = &cli.config {
        // File::from searches alternate extensions; this CLI names one exact file.
        let text = std::fs::read_to_string(path).map_err(|source| SettingsError::File {
            path: path.clone(),
            source,
        })?;
        let origin = path.to_string_lossy().into_owned();
        let values = FileFormat::Toml
            .parse(Some(&origin), &text)
            .map_err(|cause| config::ConfigError::FileParse {
                uri: Some(origin),
                cause,
            })?;
        let mut file = Config::builder();
        for (key, value) in values {
            file = file.set_override(key, value)?;
        }
        builder = builder.add_source(file.build()?);
    }
    builder = builder.add_source(
        Environment::with_prefix("SAMPLER")
            .prefix_separator("_")
            .try_parsing(false)
            .source(Some(environment)),
    );
    if let Some(interval) = cli.interval_ms {
        let origin = String::from("CLI --interval-ms");
        builder =
            builder.set_override("sample_interval_ms", Value::new(Some(&origin), interval))?;
    }
    if let Some(output) = &cli.output {
        let origin = String::from("CLI --output");
        builder = builder.set_override("output_file", Value::new(Some(&origin), output.clone()))?;
    }
    let merged = builder.build()?;
    let values = merged.collect()?;
    // config intentionally coerces floats and booleans to integers/strings. This
    // application's schema accepts only integer milliseconds and literal path strings.
    match &values["sample_interval_ms"].kind {
        ValueKind::I64(_) | ValueKind::I128(_) | ValueKind::U64(_) | ValueKind::U128(_) => {}
        ValueKind::String(value) if value.parse::<u64>().is_ok() => {}
        _ => return Err(SettingsError::InvalidInterval),
    }
    if !matches!(values["output_file"].kind, ValueKind::String(_)) {
        return Err(SettingsError::EmptyOutput);
    }
    let raw: RawSettings = merged.try_deserialize()?;
    Ok(LoadedSettings {
        settings: Settings::new(raw.sample_interval_ms, raw.output_file)?,
        interval_origin: values["sample_interval_ms"]
            .origin()
            .expect("every source, including defaults, supplies an origin")
            .to_owned(),
        output_origin: values["output_file"]
            .origin()
            .expect("every source, including defaults, supplies an origin")
            .to_owned(),
    })
}
