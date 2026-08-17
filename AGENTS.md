# AGENTS.md

This repository is a Rust application maintained under the `ilvar/strictrs` contract.

## Compatibility

- Preserve the existing Elasticsearch-compatible HTTP behavior unless a change is explicitly requested.
- Keep the PostgreSQL table shape and identifier validation compatible with the previous implementation.
- Keep passthrough behavior controlled by `PASSTHROUGH_URL` and `PASSTHROUGH_INDICES`.

## Rust / strictrs rules

- Rust toolchain: 1.97.1.
- Do not use `unsafe`.
- Do not use `unwrap`, `expect`, or unchecked indexing in production code.
- Do not use numeric `as` casts or wildcard imports.
- Public functions must declare explicit return types.
- Handle `must_use` results explicitly.
- Keep filesystem/network/process effects inside a module marked with `strictrs: capability` when direct standard-library capability APIs are required.
- Prefer explicit `Result`/`Option` handling and dependency-light code.

## Validation

Run all of these before committing or pushing:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
strictrs check .
```

Do not suppress `strictrs`, rustc, or Clippy diagnostics merely to make CI green. Fix the underlying code or document an intentional compatibility change.
