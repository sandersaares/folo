#![allow(
    clippy::type_complexity,
    reason = "builder pattern uses complex function types for flexibility"
)]

use std::hint::black_box;
use std::iter;
use std::num::NonZero;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use num_integer::Integer;

use crate::{RunMeta, ThreadPool, args};

/// A fully configured benchmark run, ready to be executed.
#[derive(derive_more::Debug)]
#[must_use]
pub struct ConfiguredRun<
    'a,
    ThreadState,
    IterState,
    MeasureWrapperState,
    MeasureOutput,
    CleanupState,
> where
    MeasureOutput: Send + 'static,
{
    pub(crate) groups: NonZero<usize>,

    #[debug(ignore)]
    pub(crate) prepare_thread_fn:
        Box<dyn Fn(args::PrepareThread<'_>) -> ThreadState + Send + Sync + 'a>,
    #[debug(ignore)]
    pub(crate) prepare_iter_fn:
        Box<dyn Fn(args::PrepareIter<'_, ThreadState>) -> IterState + Send + Sync + 'a>,

    #[debug(ignore)]
    pub(crate) measure_wrapper_begin_fn: Box<
        dyn Fn(args::MeasureWrapperBegin<'_, ThreadState>) -> MeasureWrapperState
            + Send
            + Sync
            + 'a,
    >,
    #[debug(ignore)]
    pub(crate) measure_wrapper_end_fn:
        Box<dyn Fn(MeasureWrapperState) -> MeasureOutput + Send + Sync + 'a>,

    #[debug(ignore)]
    pub(crate) iter_fn:
        Box<dyn Fn(args::Iter<'_, ThreadState, IterState>) -> CleanupState + Send + Sync + 'a>,
}

impl<ThreadState, IterState, MeasureWrapperState, MeasureOutput, CleanupState>
    ConfiguredRun<'_, ThreadState, IterState, MeasureWrapperState, MeasureOutput, CleanupState>
where
    MeasureOutput: Send + 'static,
{
    /// Executes the benchmark run on the specified thread pool for a specific number of iterations,
    /// returning the result.
    ///
    /// If you are executing the benchmark in a Criterion context, you may find it more convenient
    #[cfg_attr(
        any(test, feature = "criterion"),
        doc = "to use [`execute_criterion_on()`][Self::execute_criterion_on]."
    )]
    #[cfg_attr(
        not(any(test, feature = "criterion")),
        doc = "to use `execute_criterion_on()` (available with the `criterion` feature)."
    )]
    ///
    /// # Panics
    ///
    /// Panics if the thread pool's processor count is not divisible by the number of groups
    /// the run is configured for.
    pub fn execute_on(&self, pool: &mut ThreadPool, iterations: u64) -> RunSummary<MeasureOutput> {
        let (threads_per_group, remainder) = pool.thread_count().get().div_rem(&self.groups.get());

        assert!(
            remainder == 0,
            "thread pool processor count must be divisible by the number of groups"
        );

        let thread_count = pool.thread_count();
        let group_count = self.groups;

        let threads_per_group = NonZero::new(threads_per_group)
            .expect("guarded by NonZero thread count as well as remainder check above");

        let group_indexes = (0..self.groups.get())
            .flat_map(|group_index| iter::repeat_n(group_index, threads_per_group.get()))
            .collect::<Vec<_>>();

        // Every thread takes an item and that becomes its group index.
        let group_indexes = Arc::new(Mutex::new(group_indexes));

        // All threads will wait on this before starting, so they start together.
        let start = Arc::new(Barrier::new(thread_count.get()));

        // Break the callbacks out of `self` so we do not send `self` to the pool.
        let prepare_thread_fn = &*self.prepare_thread_fn;
        let prepare_iter_fn = &*self.prepare_iter_fn;
        let iter_fn = &*self.iter_fn;
        let measure_wrapper_begin_fn = &*self.measure_wrapper_begin_fn;
        let measure_wrapper_end_fn = &*self.measure_wrapper_end_fn;

        let results = pool.execute_task({
            let start = Arc::clone(&start);
            let group_indexes = Arc::clone(&group_indexes);

            move || {
                let group_index = group_indexes
                    .lock()
                    .expect("no worker thread panics while holding this lock")
                    .pop()
                    .expect("one group index per spawned thread");

                let meta = RunMeta::new(group_index, group_count, thread_count, iterations);

                let thread_state = prepare_thread_fn(args::PrepareThread::new(&meta));

                let iterations_usize = usize::try_from(iterations)
                    .expect("iteration count that exceeds virtual memory size is impossible to execute as state would not fit in memory");

                let iter_state = iter::repeat_with(|| {
                    prepare_iter_fn(args::PrepareIter::new(&meta, &thread_state))
                }).take(iterations_usize).collect::<Vec<_>>();

                let mut cleanup_state = Vec::with_capacity(iterations_usize);

                start.wait();

                let measure_state = measure_wrapper_begin_fn(args::MeasureWrapperBegin::new(&meta, &thread_state));

                let start_time = Instant::now();

                for iter_state in iter_state {
                    cleanup_state.push(black_box(iter_fn(args::Iter::new(&meta, &thread_state, black_box(iter_state)))));
                }

                let elapsed = start_time.elapsed();

                let measure_output = measure_wrapper_end_fn(measure_state);

                drop(cleanup_state);
                drop(thread_state);

                (elapsed, measure_output)
            }
        });

        let mut total_elapsed_nanos: u128 = 0;
        let mut measure_outputs = Vec::with_capacity(pool.thread_count().get());

        for (elapsed, measure_output) in results {
            total_elapsed_nanos = total_elapsed_nanos.saturating_add(elapsed.as_nanos());
            measure_outputs.push(measure_output);
        }

        RunSummary {
            mean_duration: calculate_mean_duration(pool.thread_count(), total_elapsed_nanos),
            measure_output: Some(measure_outputs.into_boxed_slice()),
        }
    }
}

#[cfg_attr(test, mutants::skip)] // Difficult to simulate time and therefore set expectations.
fn calculate_mean_duration(thread_count: NonZero<usize>, total_elapsed_nanos: u128) -> Duration {
    let total_elapsed_nanos_per_thread = total_elapsed_nanos
        .checked_div(thread_count.get() as u128)
        .expect("thread count is NonZero, so division by zero is impossible");

    Duration::from_nanos_u128(total_elapsed_nanos_per_thread)
}

/// The result of executing a benchmark run, carrying data suitable for ingestion
/// into a benchmark framework.
///
/// Contains timing information and any measurement data collected during the run.
/// The timing represents the mean duration across all threads, while measurement
/// outputs contain one entry per thread that participated in the run.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use many_cpus::SystemHardware;
/// use par_bench::{Run, ThreadPool};
///
/// # fn main() {
/// let mut pool = ThreadPool::new(&SystemHardware::current().processors());
/// let results = Run::new()
///     .measure_wrapper(
///         |_| std::time::Instant::now(),
///         |start_time| start_time.elapsed(),
///     )
///     .iter(|_| {
///         // Some work to measure
///         std::hint::black_box(42 * 42);
///     })
///     .execute_on(&mut pool, 1000);
///
/// // Get timing information
/// println!("Mean duration: {:?}", results.mean_duration());
///
/// // Process measurement outputs (one per thread)
/// for (i, elapsed) in results.measure_outputs().enumerate() {
///     println!("Thread {}: {:?}", i, elapsed);
/// }
/// # }
/// ```
#[derive(Debug)]
#[must_use = "the benchmarking framework will typically need this information for its results"]
pub struct RunSummary<MeasureOutput> {
    mean_duration: Duration,

    measure_output: Option<Box<[MeasureOutput]>>,
}

impl<MeasureOutput> RunSummary<MeasureOutput> {
    /// Returns the mean duration of the run.
    ///
    /// This represents the average execution time across all threads that
    /// participated in the benchmark run. The duration covers only the
    /// measured portion of the run (between measurement wrapper begin/end calls).
    ///
    /// # Examples
    ///
    /// ```
    /// use many_cpus::SystemHardware;
    /// use par_bench::{Run, ThreadPool};
    ///
    /// # fn main() {
    /// let mut pool = ThreadPool::new(&SystemHardware::current().processors());
    /// let results = Run::new()
    ///     .iter(|_| std::hint::black_box(42 + 42))
    ///     .execute_on(&mut pool, 1000);
    ///
    /// let duration = results.mean_duration();
    /// println!("Average time per iteration: {:?}", duration);
    /// # }
    /// ```
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // Real timing logic in tests is not desirable.
    pub fn mean_duration(&self) -> Duration {
        self.mean_duration
    }

    /// Returns the output of the measurement wrapper used for the run.
    ///
    /// This will iterate over one measurement output per thread that was involved in the run.
    /// The outputs are produced by the measurement wrapper end callback and can contain
    /// any thread-specific measurement data.
    ///
    /// # Panics
    ///
    /// Panics if the measure outputs have already been consumed via `take_measure_outputs()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicU64, Ordering};
    ///
    /// use many_cpus::SystemHardware;
    /// use par_bench::{Run, ThreadPool};
    ///
    /// # fn main() {
    /// let mut pool = ThreadPool::new(&SystemHardware::current().processors());
    /// let run = Run::new()
    ///     .prepare_iter(|_| Arc::new(AtomicU64::new(0)))
    ///     .measure_wrapper(
    ///         |_| (),         // Start measurement
    ///         |_state| 42u64, // Return some measurement
    ///     )
    ///     .iter(|mut args| {
    ///         let counter = args.take_iter_state();
    ///         counter.fetch_add(1, Ordering::Relaxed);
    ///     });
    ///
    /// let results = run.execute_on(&mut pool, 1000);
    ///
    /// // Each thread's measurement output
    /// for (thread_id, count) in results.measure_outputs().enumerate() {
    ///     println!("Thread {} measurement: {}", thread_id, count);
    /// }
    /// # }
    /// ```
    pub fn measure_outputs(&self) -> impl Iterator<Item = &MeasureOutput> {
        self.measure_output
            .as_ref()
            .expect("measure outputs already consumed")
            .iter()
    }

    /// Takes ownership of the measurement outputs, consuming them from the summary.
    ///
    /// Returns a boxed slice containing one measurement output per thread that
    /// participated in the benchmark run. After calling this method, subsequent
    /// calls to `measure_outputs()` or `take_measure_outputs()` will panic.
    ///
    /// This method is useful when you need to own the measurement data rather
    /// than just iterate over references to it.
    ///
    /// # Panics
    ///
    /// Panics if the measure outputs have already been consumed via a previous
    /// call to `take_measure_outputs()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use many_cpus::SystemHardware;
    /// use par_bench::{Run, ThreadPool};
    ///
    /// # fn main() {
    /// let mut pool = ThreadPool::new(&SystemHardware::current().processors());
    /// let mut results = Run::new()
    ///     .measure_wrapper(
    ///         |_| (),         // Start measurement
    ///         |_state| 42u64, // Return some measurement
    ///     )
    ///     .iter(|_| std::hint::black_box(42 * 42))
    ///     .execute_on(&mut pool, 1000);
    ///
    /// // Take ownership of all measurement outputs
    /// let owned_outputs = results.take_measure_outputs();
    /// println!("Collected {} measurements", owned_outputs.len());
    ///
    /// // Process the owned data
    /// for (i, measurement) in owned_outputs.iter().enumerate() {
    ///     println!("Thread {}: {}", i, measurement);
    /// }
    /// # }
    /// ```
    pub fn take_measure_outputs(&mut self) -> Box<[MeasureOutput]> {
        self.measure_output
            .take()
            .expect("measure outputs already consumed")
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "test code with known array bounds")]

    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::sync::LazyLock;
    use std::sync::atomic::{self, AtomicU64};

    use many_cpus::{ProcessorSet, SystemHardware};
    use new_zealand::nz;
    use static_assertions::assert_impl_all;

    assert_impl_all!(RunSummary<String>: UnwindSafe, RefUnwindSafe);

    static TWO_PROCESSORS: LazyLock<Option<ProcessorSet>> = LazyLock::new(|| {
        SystemHardware::current()
            .processors()
            .to_builder()
            .take(nz!(2))
    });

    static THREE_PROCESSORS: LazyLock<Option<ProcessorSet>> = LazyLock::new(|| {
        SystemHardware::current()
            .processors()
            .to_builder()
            .take(nz!(3))
    });

    static FOUR_PROCESSORS: LazyLock<Option<ProcessorSet>> = LazyLock::new(|| {
        SystemHardware::current()
            .processors()
            .to_builder()
            .take(nz!(4))
    });

    use super::*;
    use crate::Run;

    #[test]
    fn single_iteration_minimal() {
        let processors = SystemHardware::current()
            .processors()
            .to_builder()
            .take(nz!(1))
            .unwrap();
        let mut pool = ThreadPool::new(&processors);
        let iteration_count = Arc::new(AtomicU64::new(0));

        let _result = Run::new()
            .iter({
                let iteration_count = Arc::clone(&iteration_count);

                move |_| {
                    iteration_count.fetch_add(1, atomic::Ordering::Relaxed);
                }
            })
            .execute_on(&mut pool, 1);

        assert_eq!(iteration_count.load(atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn multiple_iterations_minimal() {
        // Repeated execution and metadata propagation need only a few distinct iterations.
        const ITERATIONS: u64 = 3;

        let processors = SystemHardware::current()
            .processors()
            .to_builder()
            .take(nz!(1))
            .unwrap();
        let mut pool = ThreadPool::new(&processors);
        let iteration_count = Arc::new(AtomicU64::new(0));
        let run_meta_seen = Arc::new(Mutex::new(None));

        let _result = Run::new()
            .prepare_thread({
                let run_meta_seen = Arc::clone(&run_meta_seen);
                move |args| {
                    *run_meta_seen.lock().unwrap() = Some(*args.meta());
                }
            })
            .iter({
                let iteration_count = Arc::clone(&iteration_count);

                move |_| {
                    iteration_count.fetch_add(1, atomic::Ordering::Relaxed);
                }
            })
            .execute_on(&mut pool, ITERATIONS);

        assert_eq!(iteration_count.load(atomic::Ordering::Relaxed), ITERATIONS);

        // Verify RunMeta contains the correct iteration count.
        let meta = run_meta_seen.lock().unwrap().unwrap();
        assert_eq!(meta.iterations(), ITERATIONS);
        assert_eq!(meta.group_index(), 0);
        assert_eq!(meta.group_count().get(), 1);
    }

    #[test]
    fn two_processors_two_groups_one_thread_per_group() {
        let Some(processors) = TWO_PROCESSORS.as_ref() else {
            println!(
                "Skipping test two_processors_two_groups_one_thread_per_group: not enough processors"
            );
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);

        let run_meta_seen = Arc::new(Mutex::new(Vec::new()));

        let _result = Run::new()
            .groups(nz!(2))
            .prepare_thread({
                let run_meta_seen = Arc::clone(&run_meta_seen);
                move |args| {
                    run_meta_seen.lock().unwrap().push(*args.meta());
                }
            })
            .iter(|_| ())
            .execute_on(&mut pool, 1);

        let mut seen = run_meta_seen.lock().unwrap();
        seen.sort_by_key(RunMeta::group_index);

        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].group_index(), 0);
        assert_eq!(seen[0].group_count().get(), 2);
        assert_eq!(seen[0].iterations(), 1);
        assert_eq!(seen[1].group_index(), 1);
        assert_eq!(seen[1].group_count().get(), 2);
        assert_eq!(seen[1].iterations(), 1);
    }
    #[test]
    fn four_processors_two_groups_two_threads_per_group() {
        let Some(processors) = FOUR_PROCESSORS.as_ref() else {
            println!(
                "Skipping test four_processors_two_groups_two_threads_per_group: not enough processors"
            );
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);

        let run_meta_seen = Arc::new(Mutex::new(Vec::new()));

        let _result = Run::new()
            .groups(nz!(2))
            .prepare_thread({
                let run_meta_seen = Arc::clone(&run_meta_seen);
                move |args| {
                    run_meta_seen.lock().unwrap().push(*args.meta());
                }
            })
            .iter(|_| ())
            .execute_on(&mut pool, 1);

        let mut seen = run_meta_seen.lock().unwrap();
        seen.sort_by_key(RunMeta::group_index);

        assert_eq!(seen.len(), 4);
        // Two threads should be in group 0.
        assert_eq!(seen[0].group_index(), 0);
        assert_eq!(seen[0].group_count().get(), 2);
        assert_eq!(seen[0].iterations(), 1);
        assert_eq!(seen[1].group_index(), 0);
        assert_eq!(seen[1].group_count().get(), 2);
        assert_eq!(seen[1].iterations(), 1);
        // Two threads should be in group 1.
        assert_eq!(seen[2].group_index(), 1);
        assert_eq!(seen[2].group_count().get(), 2);
        assert_eq!(seen[2].iterations(), 1);
        assert_eq!(seen[3].group_index(), 1);
        assert_eq!(seen[3].group_count().get(), 2);
        assert_eq!(seen[3].iterations(), 1);
    }
    #[test]
    fn three_processors_three_groups_one_thread_per_group() {
        let Some(processors) = THREE_PROCESSORS.as_ref() else {
            println!(
                "Skipping test three_processors_three_groups_one_thread_per_group: not enough processors"
            );
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);

        let run_meta_seen = Arc::new(Mutex::new(Vec::new()));

        let _result = Run::new()
            .groups(nz!(3))
            .prepare_thread({
                let run_meta_seen = Arc::clone(&run_meta_seen);
                move |args| {
                    run_meta_seen.lock().unwrap().push(*args.meta());
                }
            })
            .iter(|_| ())
            .execute_on(&mut pool, 1);

        let mut seen = run_meta_seen.lock().unwrap();
        seen.sort_by_key(RunMeta::group_index);

        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].group_index(), 0);
        assert_eq!(seen[0].group_count().get(), 3);
        assert_eq!(seen[0].iterations(), 1);
        assert_eq!(seen[1].group_index(), 1);
        assert_eq!(seen[1].group_count().get(), 3);
        assert_eq!(seen[1].iterations(), 1);
        assert_eq!(seen[2].group_index(), 2);
        assert_eq!(seen[2].group_count().get(), 3);
        assert_eq!(seen[2].iterations(), 1);
    }
    #[test]
    #[should_panic]
    fn four_processors_three_groups_panics() {
        let Some(processors) = FOUR_PROCESSORS.as_ref() else {
            println!("Skipping test four_processors_three_groups_panics: not enough processors");
            panic!("Skip test if not enough processors by panicking");
        };
        let mut pool = ThreadPool::new(processors);

        let _result = Run::new()
            .groups(nz!(3))
            .iter(|_| ())
            .execute_on(&mut pool, 1);
    }

    #[test]
    #[should_panic]
    fn one_processor_two_groups_panics() {
        let mut pool = ThreadPool::new(
            SystemHardware::current()
                .processors()
                .to_builder()
                .take(nz!(1))
                .unwrap(),
        );

        let _result = Run::new()
            .groups(nz!(2))
            .iter(|_| ())
            .execute_on(&mut pool, 1);
    }

    #[test]
    fn state_flow_from_thread_to_iteration_to_cleanup() {
        let mut pool = ThreadPool::new(
            SystemHardware::current()
                .processors()
                .to_builder()
                .take(nz!(1))
                .unwrap(),
        );
        let cleanup_states = Arc::new(Mutex::new(Vec::new()));

        let _result = Run::new()
            .prepare_thread(|_| "thread_state".to_string())
            .prepare_iter(|args| format!("{}_iter", args.thread_state()))
            .iter({
                let cleanup_states = Arc::clone(&cleanup_states);
                move |args| {
                    let cleanup_state =
                        format!("{}_{}_cleanup", args.iter_state(), args.thread_state());
                    cleanup_states.lock().unwrap().push(cleanup_state.clone());
                    cleanup_state
                }
            })
            .execute_on(&mut pool, 2);

        let states = cleanup_states.lock().unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states[0], "thread_state_iter_thread_state_cleanup");
        assert_eq!(states[1], "thread_state_iter_thread_state_cleanup");
    }

    #[test]
    fn measurement_wrapper_called_before_and_after_timed_execution() {
        let mut pool = ThreadPool::new(
            SystemHardware::current()
                .processors()
                .to_builder()
                .take(nz!(1))
                .unwrap(),
        );
        let events = Arc::new(Mutex::new(Vec::new()));

        let result = Run::new()
            .measure_wrapper(
                {
                    let events = Arc::clone(&events);
                    move |_| {
                        events.lock().unwrap().push("begin".to_string());
                        "wrapper_state".to_string()
                    }
                },
                {
                    let events = Arc::clone(&events);
                    move |wrapper_state| {
                        events.lock().unwrap().push(format!("end_{wrapper_state}"));
                        format!("output_{wrapper_state}")
                    }
                },
            )
            .iter({
                let events = Arc::clone(&events);
                move |_| {
                    events.lock().unwrap().push("iteration".to_string());
                }
            })
            .execute_on(&mut pool, 1);

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], "begin");
        assert_eq!(events[1], "iteration");
        assert_eq!(events[2], "end_wrapper_state");

        // Verify that the measure output is correctly captured and available.
        let outputs: Vec<_> = result.measure_outputs().collect();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0], "output_wrapper_state");
    }

    #[test]
    fn measure_output_threaded_through_logic() {
        let Some(processors) = TWO_PROCESSORS.as_ref() else {
            println!("Skipping test measure_output_threaded_through_logic: not enough processors");
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);

        let result = Run::new()
            .groups(nz!(2))
            .measure_wrapper(
                |args| format!("group_{}", args.meta().group_index()),
                |state| format!("{state}_output"),
            )
            .iter(|_| ())
            .execute_on(&mut pool, 1);

        let mut outputs: Vec<_> = result.measure_outputs().collect();
        outputs.sort();

        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0], "group_0_output");
        assert_eq!(outputs[1], "group_1_output");
    }

    #[test]
    fn cleanup_executed_after_measurement_wrapper_end() {
        let mut pool = ThreadPool::new(
            SystemHardware::current()
                .processors()
                .to_builder()
                .take(nz!(1))
                .unwrap(),
        );
        let events = Arc::new(Mutex::new(Vec::new()));

        let _result = Run::new()
            .measure_wrapper(|_| "wrapper_state".to_string(), {
                let events = Arc::clone(&events);
                move |_| {
                    events.lock().unwrap().push("wrapper_end".to_string());
                }
            })
            .iter({
                let events = Arc::clone(&events);
                move |_| {
                    struct CleanupTracker {
                        events: Arc<Mutex<Vec<String>>>,
                    }

                    impl Drop for CleanupTracker {
                        fn drop(&mut self) {
                            self.events.lock().unwrap().push("cleanup".to_string());
                        }
                    }

                    CleanupTracker {
                        events: Arc::clone(&events),
                    }
                }
            })
            .execute_on(&mut pool, 1);

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], "wrapper_end");
        assert_eq!(events[1], "cleanup");
    }

    fn test_call_counts(pool: &mut ThreadPool, groups: NonZero<usize>) {
        const ITERATIONS: u64 = 3;
        let expected_threads = pool.thread_count().get();

        let thread_prepare_count = Arc::new(AtomicU64::new(0));
        let iter_prepare_count = Arc::new(AtomicU64::new(0));
        let wrapper_begin_count = Arc::new(AtomicU64::new(0));
        let wrapper_end_count = Arc::new(AtomicU64::new(0));
        let iter_count = Arc::new(AtomicU64::new(0));

        let _result = Run::new()
            .groups(groups)
            .prepare_thread({
                let thread_prepare_count = Arc::clone(&thread_prepare_count);
                move |_| {
                    thread_prepare_count.fetch_add(1, atomic::Ordering::Relaxed);
                }
            })
            .prepare_iter({
                let iter_prepare_count = Arc::clone(&iter_prepare_count);
                move |_| {
                    iter_prepare_count.fetch_add(1, atomic::Ordering::Relaxed);
                }
            })
            .measure_wrapper(
                {
                    let wrapper_begin_count = Arc::clone(&wrapper_begin_count);
                    move |_| {
                        wrapper_begin_count.fetch_add(1, atomic::Ordering::Relaxed);
                    }
                },
                {
                    let wrapper_end_count = Arc::clone(&wrapper_end_count);
                    move |()| {
                        wrapper_end_count.fetch_add(1, atomic::Ordering::Relaxed);
                    }
                },
            )
            .iter({
                let iter_count = Arc::clone(&iter_count);
                move |_| {
                    iter_count.fetch_add(1, atomic::Ordering::Relaxed);
                }
            })
            .execute_on(pool, ITERATIONS);

        let expected_total_iterations = ITERATIONS.checked_mul(expected_threads as u64).unwrap();

        assert_eq!(
            thread_prepare_count.load(atomic::Ordering::Relaxed),
            expected_threads as u64
        );
        assert_eq!(
            iter_prepare_count.load(atomic::Ordering::Relaxed),
            expected_total_iterations
        );
        assert_eq!(
            wrapper_begin_count.load(atomic::Ordering::Relaxed),
            expected_threads as u64
        );
        assert_eq!(
            wrapper_end_count.load(atomic::Ordering::Relaxed),
            expected_threads as u64
        );
        assert_eq!(
            iter_count.load(atomic::Ordering::Relaxed),
            expected_total_iterations
        );
    }

    #[test]
    fn call_counts_one_processor() {
        let mut pool = ThreadPool::new(
            SystemHardware::current()
                .processors()
                .to_builder()
                .take(nz!(1))
                .unwrap(),
        );
        test_call_counts(&mut pool, nz!(1));
    }

    #[test]
    fn call_counts_four_processors_one_group() {
        let Some(processors) = FOUR_PROCESSORS.as_ref() else {
            println!("Skipping test call_counts_four_processors_one_group: not enough processors");
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);
        test_call_counts(&mut pool, nz!(1));
    }

    #[test]
    fn call_counts_two_processors_two_groups() {
        let Some(processors) = TWO_PROCESSORS.as_ref() else {
            println!("Skipping test call_counts_two_processors_two_groups: not enough processors");
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);
        test_call_counts(&mut pool, nz!(2));
    }

    #[test]
    fn call_counts_four_processors_two_groups() {
        let Some(processors) = FOUR_PROCESSORS.as_ref() else {
            println!("Skipping test call_counts_four_processors_two_groups: not enough processors");
            return; // Skip test if not enough processors.
        };
        let mut pool = ThreadPool::new(processors);
        test_call_counts(&mut pool, nz!(2));
    }
}
