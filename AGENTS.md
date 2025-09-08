# Repository Guidelines

## Project Structure & Module Organization
- `src/lib.rs`: Core FlashQ types and logic. `src/demo.rs`: interactive demo. `src/error.rs`: error types.
- `src/http/`: HTTP server/client/CLI types; binaries in `src/bin/server.rs` and `src/bin/client.rs`.
- `src/storage/`: Storage traits and backends (`memory`, `file/` with segments, index, manager).
- Tests in `tests/http/` and `tests/storage/` (integration). Benchmarks in `benches/`.
- Data samples in `data/` and `test_data/`. Docs in `docs/`.

## Build, Test, and Development Commands
- Build: `cargo build` (debug), `cargo build --release` (optimized).
- Run demo: `cargo run --bin flashq`.
- Run server: `cargo run --bin server [PORT] -- --storage=file --data-dir=./data` (defaults: port 8080, memory storage).
- Run client: `cargo run --bin client -- <cmd>`.
- Test: `cargo test` (all), `cargo test --test '*'` (integration only).
- Bench: `cargo bench`. Quick checks: `cargo check`. Lint/format: `cargo clippy`, `cargo fmt`.

### Agent Setup (Serena/Codex CLI)
- Activate the Serena project before using memories/tools: project name is `message-queue-rs`.

## Coding Style & Naming Conventions
- Use Rustfmt defaults; format with `cargo fmt` before committing. All lints must pass (`cargo clippy`).
- Naming: modules/files `snake_case`; types/traits `CamelCase`; functions/vars `snake_case`; constants `SCREAMING_SNAKE_CASE`.
- Errors: use `crate::error` types; prefer `Result<T, Error>` with context; log via `log` + `env_logger`.
- Keep modules small and cohesive; favor trait-led design for backends and I/O.

### Code & Test Style Principles
- **Code Simplicity**: ALWAYS prefer smaller functions with descriptive names and fewer comments over long functions. Break down long functions using private helpers with descriptive names.
- **Test Simplicity**: ALWAYS structure tests as: 1) Setup, 2) Action, 3) Expectation.
- **Test Atomicity**: ALWAYS write tests that validate only one behavior. If more than one behavior is being validated, split into multiple tests, each testing a single behavior.
- **Test Uniqueness**: NEVER allow two tests to validate the same behavior or logic.

## Testing Guidelines
- Integration tests live in `tests/http/` and `tests/storage/`; name files `*_tests.rs`.
- Add tests for new behavior and failure cases (ordering, recovery, validation).
- Common runs: `cargo test --lib`, `cargo test --test http_integration_tests`, `cargo test -- --nocapture` to view output.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise scope, reference issues/PRs (e.g., "Fix segment rolling (#123)").
- Before opening a PR: `cargo fmt && cargo clippy && cargo test` must be green.
- PRs should include a clear description, rationale, and any benchmarks or API changes. Call out storage/backward-compat implications (segment format, WAL, HTTP API).

## Security & Configuration Tips
- Do not run multiple servers on the same `--data-dir`; directory locks prevent corruption.
- Default port is `8080` (override via positional `[PORT]`). Linux-only `io_uring` I/O is experimental.
- For persistence, prefer `--storage=file` and set `--data-dir` to a dedicated folder under the repository or a managed path.
