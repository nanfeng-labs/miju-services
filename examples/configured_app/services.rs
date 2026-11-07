use crate::settings::Settings;
use miju_services::{Service, ServiceContext};
use std::{io, path::Path, sync::Arc, time::Instant};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    sync::Mutex,
};

/// Application-owned output. Existing files are appended to, never truncated.
pub(crate) struct SampleFile {
    writer: Mutex<Option<BufWriter<Box<dyn AsyncWrite + Unpin + Send>>>>,
}

impl std::fmt::Debug for SampleFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleFile").finish_non_exhaustive()
    }
}

impl SampleFile {
    pub(crate) async fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self::from_writer(file))
    }

    // AsyncWrite also gives tests a real I/O error/pending boundary, without a
    // special failure mode in the service or lifecycle code.
    pub(crate) fn from_writer(writer: impl AsyncWrite + Unpin + Send + 'static) -> Self {
        Self {
            writer: Mutex::new(Some(BufWriter::new(Box::new(writer)))),
        }
    }

    pub(crate) async fn record(&self, line: &str) -> io::Result<()> {
        let mut writer = self.writer.lock().await;
        let writer = writer
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "sample file is closed"))?;
        writer.write_all(line.as_bytes()).await
    }

    pub(crate) async fn close(&self) -> io::Result<()> {
        // Taking the writer closes admission to writes. The cleanup future owns it
        // until flush completes, including when shutdown observation returns Pending.
        let mut writer = self.writer.lock().await.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "sample file close already taken")
        })?;
        writer.flush().await
    }
}

pub(crate) struct Sampler {
    settings: Settings,
    output: Arc<SampleFile>,
}

impl Sampler {
    pub(crate) fn new(settings: Settings, output: Arc<SampleFile>) -> Self {
        Self { settings, output }
    }
}

impl Service for Sampler {
    type Error = io::Error;

    async fn run(self, context: ServiceContext) -> Result<(), Self::Error> {
        let started = Instant::now();
        context.ready();
        loop {
            tokio::select! {
                biased;
                _ = context.stopping() => break,

                _ = tokio::time::sleep(self.settings.interval()) => {
                    let sample = format!(
                        "elapsed_ms={} process_id={}\n",
                        started.elapsed().as_millis(),
                        std::process::id(),
                    );
                    self.output.record(&sample).await?;
                }
            }
        }
        Ok(())
    }
}
