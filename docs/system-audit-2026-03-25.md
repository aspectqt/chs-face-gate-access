# System Audit Report (2026-03-25)

## Scope

- Repository structure and tracked artifact hygiene
- Backend startup/runtime verification
- MongoDB connectivity and index bootstrap behavior
- Core route smoke checks for dashboard, analytics, students, gate logs, and SMS logs
- Existing verification surface and missing automated tests

## Findings

### Fixed

1. MongoDB startup emitted repeated index warnings during module import because `ensure_indexes()` attempted to recreate indexes that already existed under auto-generated names with slightly different options.
2. The backend README documented a `tests/` suite that did not exist, so the published test command was inaccurate.
3. Generated analysis output and local TLS certificate files were not ignored by git, which made accidental commits more likely.

### Confirmed During Audit

1. Local MongoDB connectivity is working against `face_gate_db`.
2. Core authenticated routes render successfully for a Full Admin session: `/dashboard`, `/analytics`, `/students`, `/gate-logs`, `/sms-logs`.
3. Dashboard and health APIs return successful JSON responses under an authenticated session.
4. Staff authorization for `/analytics` correctly redirects back to `/dashboard`.
5. PHILSMS auth health reports token configuration as present, but outbound network-dependent auth/balance probes can still fail in restricted environments.

## Changes Applied

1. Hardened index creation in `backend/config.py` to detect existing indexes before attempting creation and to fall back gracefully after conflict errors.
2. Added `backend/tests/` with service-level unit tests and app smoke tests.
3. Updated `backend/README.md` so the documented test command matches the repository.
4. Extended `.gitignore` to exclude generated analysis output and local certificate material.

## Remaining Risks / Recommendations

1. `backend/app.py` is still a very large monolith and remains the main maintainability bottleneck. The next refactor should split auth/profile, analytics, student management, and scan endpoints into blueprints or modules.
2. Several local scratch/debug files exist outside the git repo root (`C:\Capstone Project`). They should be archived or deleted once the team decides which ones still have operational value.
3. Untracked files inside the repo, such as local certs and helper scripts, should be reviewed before commit so production code and local machine artifacts stay separate.
4. External SMS integration should be validated again from an environment with outbound access, since sandboxed/local restrictions can produce false negatives for provider reachability.
