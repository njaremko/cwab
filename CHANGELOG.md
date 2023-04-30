# 0.5.11

- Add backtraces to anyhow errors
- Documentation updates

# 0.5.10

- Async middleware
- Fixed a bug that caused the librarian to burn through CPU cycles needlessly when no queues were specified
- Added support for specifying queue namespaces in TOML configuration, or as a command line argument
- Improved ergonomics of job result type
- Renamed "Completed" queue to "Processed"
- Added a default retry policy
- Store job error messages when they occur

# 0.5.4

- Just a small documentation and README change

# 0.5.3

- Changed registering jobs to not return a result. It can't fail.

# 0.5.2
- Published the `ClientMiddleware` trait

# 0.5.0

### Launching Cwab Pro
- [x] Batched jobs
- [x] Cron support
- [x] Expiring jobs
- [x] Unique jobs
- [x] Encryption
- [x] Rate limiting
- [x] Parallelism
- [x] Web UI
- [x] Metrics
- [x] Dedicated support
- [x] A commercial license
