use crate::metrics::{ReportBuilder, ReportPage};
use crossbeam::channel;

/// Collects metrics from a channel and publishes a report when dropped. This is used by the Folo
/// entrypoint macro when metrics publishing is enabled. It is not meant for direct consumption.
pub struct MetricsCollector {
    metrics_rx: channel::Receiver<ReportPage>,
    metrics_tx: channel::Sender<ReportPage>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let (metrics_tx, metrics_rx) = channel::unbounded();

        Self {
            metrics_rx,
            metrics_tx,
        }
    }

    pub fn tx(&self) -> channel::Sender<ReportPage> {
        self.metrics_tx.clone()
    }

    fn publish_report(&mut self) {
        let mut report_builder = ReportBuilder::new();

        // We do not wait for any of the channels to send data, as we expect this to be called when
        // the runtime is shut down - if a thread did not send by now, it never will. Also, note
        // that we are ourselves holding a tx, so the channel will never close.
        for page in self.metrics_rx.try_iter() {
            report_builder.add_page(page);
        }

        let report = report_builder.build();

        println!("{report}");
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MetricsCollector {
    fn drop(&mut self) {
        self.publish_report();
    }
}
