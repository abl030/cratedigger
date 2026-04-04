# Memory Index

## User
- [user_multi_device.md](user_multi_device.md) — Browses music.ablz.au from 4+ devices; client-side caching is useless

## Feedback
- [feedback_no_virtiofs_blame.md](feedback_no_virtiofs_blame.md) — Don't blame virtiofs for DB corruption; beets DB works fine on the same filesystem
- [feedback_dont_dismiss_infrastructure.md](feedback_dont_dismiss_infrastructure.md) — Don't dismiss proper infra as overkill; Postgres >> SQLite, nspawn makes services cheap
- [feedback_fix_not_refactor.md](feedback_fix_not_refactor.md) — Fix bugs minimally; don't expand into refactoring unless explicitly asked
- [feedback_finish_the_job.md](feedback_finish_the_job.md) — Always wire up features end-to-end; don't build infrastructure and leave it disconnected

## Project
- [project_postgres_migration_done.md](project_postgres_migration_done.md) — Pipeline DB migrated to PostgreSQL nspawn container (2026-03-25)
- [project_music_web_ui.md](project_music_web_ui.md) — music.ablz.au web UI for album requests (MVP shipped 2026-03-25)
- [project_postgres_next_steps.md](project_postgres_next_steps.md) — Remaining tasks: ephemeral test DB improvements, portable state dir
- [project_disambiguate_page.md](project_disambiguate_page.md) — Disambiguate tab: tier-based coverage, pressing details, library mgmt (2026-04-01)
- [project_manual_import.md](project_manual_import.md) — Manual import from Complete folder MVP, needs UX refinement (2026-04-01)
- [project_audio_quality_types.md](project_audio_quality_types.md) — AQM type system deployed; AudioQualityState deferred until needed
