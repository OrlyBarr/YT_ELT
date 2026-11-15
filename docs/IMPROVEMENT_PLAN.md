# YT_ELT Improvement Plan

Findings from a project review on 9 November 2026. Measurements are from that date.

**Effort scale:** S = under 2 hours. M = about half a day to a day. L = several days.

## P0: Fix first

- [ ] **1. Rotate the exposed API key.** Effort: S
  - Why: an exposed key can be used by anyone and can use up the project's quota.
- [ ] **2. Change view, like and comment counts to `BIGINT` in both schemas, with an `ALTER TABLE` for the existing tables.** Effort: S
  - Why: the top video is at 91% of the `INT` limit, so loads will fail around May 2027.
- [ ] **3. Stop before any delete if a pull is empty or far smaller than the table, and add a Soda row count check.** Effort: S
  - Why: a bad API response could wipe the warehouse while every check still passes.

## P1: Reliability and correctness

- [ ] **4. Check each layer before loading the next: staging, check, core, check, dashboard.** Effort: M
  - Why: a failed staging check cannot currently stop bad data reaching core. Do this together with item 17.
- [ ] **5. Add Soda checks for row count, missing titles and dates, negative counts, ID length, valid `Video_Type` values and expected columns.** Effort: S
  - Why: the current four checks cover only IDs and two ratios. Add a freshness check after item 9.
- [ ] **6. Add retries with backoff and request timeouts, and move `dagrun_timeout` and `max_active_runs` onto each `DAG(...)`.** Effort: S
  - Why: one temporary error loses a day, calls can hang, and Airflow ignores those two settings in `default_args`.
- [ ] **7a. Add a failure alert by email or Slack.** Effort: S
  - Why: failures are currently silent.
- [ ] **7b. Move the pipeline to an always-on server if a continuous daily history is needed.** Effort: M to L
  - Why: there are 18 pulls in 117 days, with gaps of up to 23 days, and missed days cannot be recovered.
- [ ] **8. Load each table in one transaction using `INSERT ... ON CONFLICT ("Video_ID") DO UPDATE`.** Effort: M
  - Why: committing row by row can leave tables half updated, and a changed publish date silently stops updates for that video.
- [ ] **9. Add a `core.video_stats_daily` history table, a soft-delete flag and `loaded_at`, backfill from the JSON extracts, and back up the data.** Effort: M
  - Why: past values exist only in local JSON files, and the warehouse overwrites them on every run.
- [ ] **10. Base file paths on the run's logical date and write the JSON atomically.** Effort: S to M
  - Why: re-running an old load reads today's file, so runs cannot be reproduced or backfilled.
- [ ] **11. In CI, build, test, then push using the commit SHA tag. Add the missing paths to the test filter and pin third-party actions to full SHAs.** Effort: S
  - Why: images are published before testing, some changes run no tests, and a movable action tag can reach the job's secrets.

## P2: Quality and maintainability

- [ ] **12. Add tests for duration parsing, transformations, the empty-pull guard, deletes, upserts and `load_samples`.** Effort: M
  - Why: most current tests check only mocks, HTTP 200 or `SELECT 1`.
- [ ] **13. Fix the error handler in `delete_rows` and use `WHERE "Video_ID" = ANY(%s)`.** Effort: S
  - Why: the handler raises a `TypeError` that hides the real database error.
- [ ] **14. Send the API key in the `X-Goog-Api-Key` header and read Airflow variables inside tasks.** Effort: S
  - Why: request errors include the full URL, and one missing variable breaks all three DAGs at import.
- [ ] **15. Remove the hard-coded `elt_db`, replace the manual `env_file` toggling, and install with the Airflow constraints file.** Effort: S
  - Why: config can drift between local and CI, and image builds are not reproducible.
- [ ] **16. Store duration as integer seconds, use `TIMESTAMPTZ`, and document `Video_Type` as a heuristic.** Effort: S to M
  - Why: `TIME` wraps at 24 hours, and the 60-second Shorts rule may misclassify newer videos of up to 3 minutes.
- [ ] **17. Merge the three chained DAGs into one and update the task-count test and CI loop.** Effort: M
  - Why: a green DAG only means the next one was triggered, not that it succeeded. This also simplifies items 4 and 10.
- [ ] **18. Pass a file path through XCom instead of the dataset, and schedule `airflow db clean` and log cleanup.** Effort: S
  - Why: 140 XCom rows hold 3.9 MB in the metadata database, and nothing cleans them up.

## P3: Polish

- [ ] **19. Remove no-op `try/except` blocks, commented-out code and the broken `__main__` block, and merge `staging_table` and `core_table`.** Effort: S to M
  - Why: dead and duplicated code makes changes harder and riskier.
- [ ] **20. Add a dashboard test and consider bundling Chart.js locally.** Effort: S
  - Why: the dashboard is untested and needs a CDN. Items 3 and 9 cover its empty-overwrite risk and its file dependency.
- [ ] **21. Add a runbook for failures, re-runs and key rotation, document UTC handling, and correct `CLAUDE.md`.** Effort: S
  - Why: recovery steps are undocumented, and `CLAUDE.md` has a typo and implies tests always follow an image build.

## Suggested order

1. Immediately: item 1.
2. First session: items 2, 3, 5, 6, 13 and 7a.
3. Next: items 8, 17 and 4 together, then 10 and 11.
4. Then: item 9, followed by item 12.
5. As time allows: items 7b, 14, 15, 16, 18 and all of P3.

Performance needs no work at the current size of about 1,012 rows and about 43 API quota units a day. Item 8 removes the only real waste.
