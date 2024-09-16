# Folo task and IO runtime for Rust

This is an experimental runtime for Rust services, focusing on thread-isolated execution and only
supporting high-performance I/O primitives (IOCP, io_uring, ...). Primarily for learning purposes.

# Design tenets

* **Isolation** - one logical activity (job/request/...) is bound to one specific processor (CPU
  core) and one specific async worker thread. This ensures high data locality and processor cache
  efficiency, making every operation just that little bit faster. Blocking tasks are spawned on the
  same processor as the async tasks that originated them.
* **High performance asynchronous I/O** - only use the top performing asynchronous I/O APIs offered
  by each operating system. This means I/O Completion Ports on Windows and io_uring on Linux.
* **Zero-copy I/O** - the operating system writes directly into the final buffer the data needs to be in
  or reads directly from the original buffer the data was supplied in.

# Features

| Name                       | Status  |
|----------------------------|---------|
| Async task execution       | ✅       |
| Blocking task execution    | Partial |
| Compute task execution     | ❌       |
| Filesystem primitives      | Minimal |
| Network primitives         | Minimal |
| Synchronization primitives | Minimal |
| Time primitives            | ✅       |
| Windows                    | ✅       |
| Linux                      | ❌       |
| `no_std`                   | ❌       |
| Test suite                 | Minimal |
| Benchmarks                 | Minimal |
| Documentation              | ❌       |
