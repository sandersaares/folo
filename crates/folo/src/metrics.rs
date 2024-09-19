use crate::time::LowPrecisionInstant;
use negative_impl::negative_impl;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell, UnsafeCell},
    cmp,
    collections::HashMap,
    fmt::{Display, Write},
    future::Future,
    rc::Rc,
    time::Duration,
};

pub type Magnitude = i64;

/// Measures the rate and amplitude of events. Just create an instance via EventBuilder and start
/// feeding it events. It will do the rest. Interior mutability is used, so you can put these in
/// thread-local static variables for ease of use.
///
/// # Thread safety
///
/// This type is single-threaded. Create a separate instance for each thread.
/// The data will be merged across all threads to yield a combined report.
pub struct Event {
    bag: Rc<ObservationBag>,
}

impl Event {
    /// Observes an event with a magnitude of 1. An event that only takes observations of this kind
    /// is a counter and undergoes simplified reporting.
    pub fn observe_unit(&self) {
        self.bag.insert(1, 1);
    }

    pub fn observe(&self, magnitude: Magnitude) {
        self.bag.insert(magnitude, 1);
    }

    pub fn observe_millis(&self, duration: Duration) {
        self.bag.insert(duration.as_millis() as i64, 1);
    }

    pub fn observe_many(&self, magnitude: Magnitude, count: usize) {
        self.bag.insert(magnitude, count);
    }

    pub fn observe_duration_millis<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        // We do not use ultra-low precision here because this is a synchronous call in a
        // situation where the async task engine could not possible update the ultra low precision
        // instant, so it would always record an elapsed time of zero.
        let start = LowPrecisionInstant::now();

        let result = f();

        self.observe_millis(start.elapsed());

        result
    }

    pub async fn observe_duration_millis_async<F, FF, R>(&self, f: F) -> R
    where
        F: FnOnce() -> FF,
        FF: Future<Output = R>,
    {
        // We do not use ultra-low precision here because this is a synchronous call in a
        // situation where the async task engine could not possible update the ultra low precision
        // instant, so it would always record an elapsed time of zero.
        let start = LowPrecisionInstant::now();

        let result = f().await;

        self.observe_millis(start.elapsed());

        result
    }

    fn new(bag: Rc<ObservationBag>) -> Self {
        Self { bag }
    }
}

#[negative_impl]
impl !Send for Event {}
#[negative_impl]
impl !Sync for Event {}

pub struct EventBuilder {
    name: Cow<'static, str>,

    /// Upper bounds of histogram buckets to use. May be empty if histogram not meaningful.
    buckets: &'static [Magnitude],
}

impl EventBuilder {
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            buckets: &[],
        }
    }

    pub fn buckets(mut self, buckets: &'static [Magnitude]) -> Self {
        self.buckets = buckets;
        self
    }

    pub fn build(self) -> Event {
        let bag = BAGS.with_borrow_mut(|bags| {
            Rc::clone(
                bags.entry(self.name.to_string())
                    .or_insert_with(|| Rc::new(ObservationBag::new(self.buckets))),
            )
        });

        Event::new(bag)
    }
}

thread_local! {
    static BAGS: RefCell<HashMap<String, Rc<ObservationBag>>> = RefCell::new(HashMap::new());
}

/// Collects all the observations made about a particular event and processes the data for analysis.
///
/// Data from different bags of the same event is merged together to yield a combined report later.
struct ObservationBag {
    count: Cell<usize>,
    sum: Cell<Magnitude>,

    // This is UnsafeCell because it is part of some very hot loops and
    // we do not want to pay for the runtime borrow checking.
    bucket_counts: UnsafeCell<Vec<usize>>,

    bucket_magnitudes: &'static [Magnitude],
}

impl ObservationBag {
    fn insert(&self, magnitude: Magnitude, count: usize) {
        self.count.set(self.count.get() + count);
        self.sum
            .set(self.sum.get() + magnitude * (count as Magnitude));

        // SAFETY: This is a single threaded type and we do not let any bucket references escape
        // the type while it may still be mutated, so we can be certain that references are legal.
        let bucket_counts = unsafe { &mut *self.bucket_counts.get() };

        // This may be none if we have no buckets (i.e. it is a counter, not histogram).
        if let Some(bucket_index) =
            self.bucket_magnitudes
                .iter()
                .enumerate()
                .find_map(|(i, &bucket_magnitude)| {
                    if magnitude <= bucket_magnitude {
                        Some(i)
                    } else {
                        None
                    }
                })
        {
            bucket_counts[bucket_index] += count;
        }
    }

    fn new(buckets: &'static [Magnitude]) -> Self {
        Self {
            count: Cell::new(0),
            sum: Cell::new(0),
            bucket_counts: UnsafeCell::new(vec![0; buckets.len()]),
            bucket_magnitudes: buckets,
        }
    }

    fn snapshot(&self) -> ObservationBagSnapshot {
        ObservationBagSnapshot {
            count: self.count.get(),
            sum: self.sum.get(),
            // SAFETY: This is a single-threaded type and we never let any exclusive reference
            // escape from this type, so taking this reference is legal.
            bucket_counts: unsafe { &*self.bucket_counts.get() }.clone(),
            bucket_magnitudes: self.bucket_magnitudes,
        }
    }
}

struct ObservationBagSnapshot {
    count: usize,
    sum: Magnitude,
    bucket_counts: Vec<usize>,
    bucket_magnitudes: &'static [Magnitude],
}

impl ObservationBagSnapshot {
    fn merge(&mut self, other: &ObservationBagSnapshot) {
        self.count += other.count;
        self.sum += other.sum;

        // Briefest sanity check. We just assume the magnitudes are the same.
        assert!(self.bucket_counts.len() == other.bucket_counts.len());

        for (i, &other_bucket_count) in other.bucket_counts.iter().enumerate() {
            self.bucket_counts[i] += other_bucket_count;
        }
    }
}

/// A report page is a single thread's contribution to a report. Collect all the pages from all
/// the threads and you can assemble a report to show to the operator or to export.
pub struct ReportPage {
    bags: HashMap<String, ObservationBagSnapshot>,
}

/// Assembles a report page representing the latest state of observations on the current thread.
pub fn report_page() -> ReportPage {
    ReportPage {
        bags: BAGS.with_borrow(|bags| {
            bags.iter()
                .map(|(name, bag)| (name.clone(), bag.snapshot()))
                .collect()
        }),
    }
}

pub struct ReportBuilder {
    pages: Vec<ReportPage>,
}

impl Default for ReportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReportBuilder {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    pub fn add_page(&mut self, page: ReportPage) {
        self.pages.push(page);
    }

    pub fn build(self) -> Report {
        let merged_snapshots = self.pages.into_iter().map(|page| page.bags).fold(
            HashMap::new(),
            |mut merged, bags| {
                for (name, snapshot) in bags {
                    merged
                        .entry(name.clone())
                        .or_insert_with(|| ObservationBagSnapshot {
                            count: 0,
                            sum: 0,
                            bucket_counts: vec![0; snapshot.bucket_counts.len()],
                            bucket_magnitudes: snapshot.bucket_magnitudes,
                        })
                        .merge(&snapshot);
                }

                merged
            },
        );

        Report {
            bags: merged_snapshots,
        }
    }
}

/// An analysis of collected data, designed for display to console output.
pub struct Report {
    bags: HashMap<String, ObservationBagSnapshot>,
}

impl Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Sort by name for consistent output.
        let mut sorted_bags: Vec<_> = self.bags.iter().collect();
        sorted_bags.sort_by_key(|(name, _)| name.as_str());

        for (name, snapshot) in sorted_bags {
            writeln!(f, "{}: {}", name, snapshot)?;
        }

        Ok(())
    }
}

impl Display for ObservationBagSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.count as Magnitude == self.sum {
            writeln!(f, "{} (counter)", self.count)?;
        } else if self.count > 0 {
            writeln!(
                f,
                "{}; sum {}; avg {}",
                self.count,
                self.sum,
                self.sum / self.count as Magnitude
            )?;
        } else {
            writeln!(f, "0")?;
            return Ok(());
        }

        // In general, a metric with count 0 is rarely going to even be displayed because they are
        // lazy-initialized, so if not accessed how was it even created. But let's be thorough.
        if self.bucket_counts.is_empty() || self.count == 0 {
            return Ok(());
        }

        let mut buckets_cumulative = 0;
        let max_bucket_value = self
            .bucket_counts
            .iter()
            .max()
            .expect("we verified already that at least one bucket exists");

        let mut buckets_to_print = self
            .bucket_counts
            .iter()
            .enumerate()
            .map(|(index, &count)| {
                buckets_cumulative += count;

                let magnitude = self.bucket_magnitudes[index];

                (Some(magnitude), count)
            })
            .collect::<Vec<_>>();

        let plus_infinity_count = self.count - buckets_cumulative;

        const TOTAL_BAR_WIDTH: usize = 50;
        let count_per_char = cmp::max(
            cmp::max(max_bucket_value, &plus_infinity_count) / TOTAL_BAR_WIDTH,
            1,
        );

        buckets_to_print.push((None, plus_infinity_count));

        // Measure the dynamic parts of the string to know how much padding to add.

        let mut count_str = String::new();

        let widest_count = buckets_to_print.iter().fold(0, |n, b| {
            count_str.clear();
            write!(&mut count_str, "{}", b.1).unwrap();
            cmp::max(n, count_str.len())
        });

        let mut end_str = String::new();
        let widest_range = buckets_to_print.iter().fold(0, |n, b| {
            end_str.clear();

            match b.0 {
                Some(le) => write!(&mut end_str, "{}", le).unwrap(),
                None => write!(&mut end_str, "+inf").unwrap(),
            }

            cmp::max(n, end_str.len())
        });

        for bucket in buckets_to_print.into_iter() {
            end_str.clear();

            match bucket.0 {
                Some(le) => write!(&mut end_str, "{}", le).unwrap(),
                None => write!(&mut end_str, "+inf").unwrap(),
            }

            for _ in 0..widest_range - end_str.len() {
                end_str.insert(0, ' ');
            }

            count_str.clear();
            write!(&mut count_str, "{}", bucket.1).unwrap();
            for _ in 0..widest_count - count_str.len() {
                count_str.insert(0, ' ');
            }

            write!(f, "value <= {} [ {} ]: ", end_str, count_str)?;
            for _ in 0..bucket.1 / count_per_char {
                write!(f, "∎")?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn event_smoke_test() {
        clear();

        let event = EventBuilder::new("test")
            .buckets(&[0, 1, 2, 3])
            .build();

        event.observe(1);
        event.observe(2);
        event.observe(3);
        event.observe(4);
        event.observe(5);
        event.observe_many(1, 2);
        event.observe_many(2, 3);
        event.observe_many(3, 4);
        event.observe_many(4, 5);
        event.observe_many(5, 6);

        let page = report_page();

        assert_eq!(page.bags.len(), 1);

        let snapshot = page.bags.get("test").unwrap();
        assert_eq!(snapshot.count, 25);
        assert_eq!(snapshot.sum, 85);
        assert_eq!(snapshot.bucket_counts, vec![0, 3, 4, 5]);

        let mut report_builder = ReportBuilder::new();
        report_builder.add_page(page);

        let report = report_builder.build();

        println!("{}", report);
    }

    #[test]
    fn counter() {
        clear();

        let event = EventBuilder::new("test_counter")
            .buckets(&[])
            .build();

        event.observe_unit();
        event.observe_unit();
        event.observe_unit();

        let page = report_page();

        assert_eq!(page.bags.len(), 1);

        let snapshot = page.bags.get("test_counter").unwrap();
        assert_eq!(snapshot.count, 3);
        assert_eq!(snapshot.sum, 3);
        assert_eq!(snapshot.bucket_counts, Vec::<usize>::new());

        let mut report_builder = ReportBuilder::new();
        report_builder.add_page(page);

        let report = report_builder.build();

        println!("{}", report);
    }

    #[test]
    fn multi_page_report() {
        clear();

        let event = EventBuilder::new("test")
            .buckets(&[1, 2, 3])
            .build();

        event.observe(0);
        event.observe(100);

        let other_page = thread::spawn(move || {
            let event = EventBuilder::new("test")
                .buckets(&[1, 2, 3])
                .build();

            event.observe(-10);
            event.observe(1);
            event.observe(1);
            event.observe(1);

            report_page()
        })
        .join()
        .unwrap();

        let this_page = report_page();

        let mut report_builder = ReportBuilder::new();
        report_builder.add_page(this_page);
        report_builder.add_page(other_page);

        let report = report_builder.build();

        let snapshot = report.bags.get("test").unwrap();

        assert_eq!(snapshot.count, 6);
        assert_eq!(snapshot.sum, 93);
        assert_eq!(snapshot.bucket_counts, vec![5, 0, 0]);

        println!("{}", report);
    }

    #[test]
    fn multi_metric_report() {
        clear();

        let event = EventBuilder::new("test")
            .buckets(&[1, 2, 3])
            .build();

        event.observe(0);
        event.observe(100);

        let event = EventBuilder::new("another_test").build();

        event.observe(1234);
        event.observe(45678);

        let event = EventBuilder::new("more").build();

        event.observe(1234);
        event.observe(45678);

        let mut report_builder = ReportBuilder::new();
        report_builder.add_page(report_page());

        let report = report_builder.build();

        assert_eq!(3, report.bags.len());

        println!("{}", report);
    }

    fn clear() {
        BAGS.with_borrow_mut(|bags| bags.clear());
    }
}
