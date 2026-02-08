# Changelog

## [Unreleased]

- Removed ThreadPoolExecutor wait approach (`_wait_executor`) attribute, streamlining concurrency management.
- Added `disable_batch_submission` boolean parameter to `DragonExecutionBackendV3.__init__` and `create` methods, allowing configuration of task submission strategy.
- Introduced `_monitored_batches` dictionary and `_batch_monitor_thread` to manage and monitor active task batches.
- Replaced the `_wait_for_batch` method with a new `_monitor_loop` (running in a dedicated thread) and `_process_batch_results` helper, implementing a polling-based mechanism for task completion.
- Modified `_async_init` to start the `_batch_monitor_thread` during backend initialization.
- Updated `submit_tasks` to support conditional task submission: tasks can now be submitted as individual batches (stream mode) or a single combined batch, based on the `_disable_batch_submission` flag.
- Adjusted the `shutdown` method to properly manage the lifecycle of the new `_batch_monitor_thread`.
