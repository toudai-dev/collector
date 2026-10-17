# Agent Guide

This file provides guidance to AI coding agents working in this repository.

## Commit Titles

Use conventional commit titles for any commit you create. Pull request titles are validated against the same convention by `.github/workflows/commits.yml`, so a non-conventional title blocks the pull request.

Use only these types, which are the ones the workflow accepts:

- `feat`
- `fix`
- `doc` (`docs` is also accepted)
- `perf`
- `refactor`
- `style`
- `test`
- `chore`
- `ci`
- `revert`
- `security`

Use standard conventional commit formatting such as `fix(exporter): handle empty batches` or `ci: pin the Go toolchain`.

## Working Style

- Prefer minimal, targeted changes that preserve the existing code style.
- Run relevant tests for the area you touch when practical.
- Do not introduce a second commit-title convention; keep commit types aligned with `.github/workflows/commits.yml`.
- Commit in small logical chunks. This is a hard requirement when creating commits.
- Each commit must be self-reviewable: it should contain one coherent change, include its relevant tests/docs, and avoid mixing unrelated cleanup or follow-up work.
- Prefer writing table tests over individual tests whenever possible
- Add comments that describe intent for non-obvious logic. This is required when code encodes a production constraint, workaround, performance tradeoff, safety guardrail, or surprising behavior.
- Keep comments succinct and useful for review. Avoid comments that merely restate what the next line of code does.

## Development Commands

- `make build` builds `bin/toudai-collector`
- `make vet` runs `go vet ./...`
- `make test` runs `go test ./...`
