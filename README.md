# community-intel-engine

Multi-agent pipeline that turns Discord, Reddit, and GitHub feedback into structured bug reports and feature requests

## Status

Ingestion is built and locally verified. The classification, dedup, and
ticket-drafting pipeline is specified but not yet implemented.

- Design: [`docs/superpowers/specs/2026-09-08-community-intel-engine-design.md`](docs/superpowers/specs/2026-09-08-community-intel-engine-design.md)
- Plan: [`docs/superpowers/plans/2026-09-08-foundation-and-ingestion.md`](docs/superpowers/plans/2026-09-08-foundation-and-ingestion.md)

## Running locally

Three modes. Only the third needs configuration.

### 1. Unit tests — no setup

```bash
uv sync --all-extras
uv run pytest
```

### 2. End-to-end smoke test — no setup

Drives real GitHub webhook payloads and a stubbed Reddit listing through the
actual Lambda handlers, with `moto` standing in for DynamoDB and SQS. No AWS
account, no credentials, no configuration.

```bash
uv run python scripts/local_smoke.py
```

It reports what was stored versus enqueued and exits non-zero on failure, so
it works as a pre-deploy gate.

### 3. Against real AWS — needs `.env`

```bash
cp .env.example .env
```

Fill in `CIE_TABLE_NAME` and `CIE_QUEUE_URL` from the `cdk deploy` outputs.
Every setting is documented in [`.env.example`](.env.example).

Two things worth knowing:

- **A shell variable beats `.env`**, so you can override one value for a single
  run without editing the file.
- **AWS credentials do not come from `.env`.** They use the normal chain —
  `~/.aws/credentials`, `AWS_PROFILE`, or SSO.

`.env` is gitignored. `.env.example` is not, and carries no real values.

## Building and deploying

```bash
./scripts/build_lambda.sh    # packages Lambda without Docker, then verifies
uv run cdk synth
uv run cdk deploy
```

The build cross-compiles for `linux/x86_64`, so the bundle is deliberately not
importable on a macOS or ARM machine. `scripts/verify_bundle.py` checks it
structurally instead and runs automatically as part of the build.

## Layout

```
src/community_intel/
├── adapters/      github.py, reddit.py → NormalizedItem
├── handlers/      webhook.py (API Gateway), reddit_poll.py (EventBridge)
├── prefilter.py   pure rejection predicate; gates the SQS enqueue
├── store.py       DynamoDB + SQS
├── models.py      NormalizedItem
└── config.py      settings
infra/             CDK — the only place AWS resources are declared
scripts/           build, bundle verification, local smoke test
```
