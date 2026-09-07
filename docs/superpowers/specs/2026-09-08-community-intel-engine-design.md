# Community Intelligence Engine — Design

**Date:** 2026-09-08
**Status:** Approved for planning

## Problem

Community feedback arrives scattered and unstructured. One bug may surface as a
rambling Reddit post, a terse GitHub issue, and a throwaway comment on an
unrelated thread. Someone has to notice the pattern, judge severity, and write
it up. That triage is slow, misses things, and lets real product signal die in
the scroll.

## Solution

A serverless pipeline that ingests GitHub and Reddit activity, discards noise,
classifies the rest as bug or feature request, merges duplicate reports into
clusters, and files a structured GitHub issue for each genuinely new cluster.
Subsequent reports of a known problem become evidence comments on the issue that
already exists.

## Scope

**In scope for v1**

- GitHub ingestion: `issues`, `issue_comment`, `discussion`, `discussion_comment`
- Reddit ingestion: submissions and comments from configured subreddits
- Classification into bug / feature / noise
- Deduplication by structural parentage and semantic similarity
- Ticket drafting and automatic filing to GitHub Issues
- Evidence comments on existing issues for duplicate reports

**Explicitly out of scope**

- Discord. Its Gateway is a persistent WebSocket that cannot run on Lambda, and
  it was the only component forcing an always-on container. Deferred, not
  discarded: the adapter interface accepts a Fargate-hosted Discord adapter
  later with no pipeline changes.
- Slack digests. GitHub Issues is the single output sink.
- Human approval before filing. Tickets are auto-filed.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Compute | Fully serverless (Lambda) | No component requires a persistent connection once Discord is out |
| Orchestration | LangGraph | Three terminal paths and per-node tracing; branching grows over time |
| Records | DynamoDB, single table, no GSIs | SQS carries the work queue, removing the only query that needed an index |
| Vectors | S3 Vectors | GA, cost-optimised, no provisioned infrastructure |
| Embeddings | Bedrock Titan v2, 1024-dim, fixed | Immutable per index; encoded in the index name |
| Chat models | Provider-configurable via `init_chat_model` | Bedrock deployed, Ollama locally, no code change |
| Review gate | None | Explicit product decision; filing is automatic |
| Secrets | SSM Parameter Store SecureString | Secrets Manager's rotation is unused at $0.40/secret/month |
| Evals | pytest over JSONL | No third-party service; runs in CI |

## Architecture

```
GitHub webhook ──► API Gateway ──► webhook Lambda ──┐
                                                    ├──► DynamoDB + SQS
EventBridge (2 min) ──► reddit_poll Lambda ─────────┘
                                                          │
                                     SQS ──► pipeline Lambda ──► Bedrock
                                                          │      S3 Vectors
                                                          └──► GitHub Issues
```

No VPC, no NAT gateway, no containers, no always-on compute.

### Why each source uses the compute it does

Delivery model, not preference. GitHub pushes over HTTP, so it is webhook plus
Lambda. Reddit has no webhooks and PRAW's `stream.*` is a polling loop with a
sleep, so a scheduled Lambda gets identical data with free retries and no
always-on cost. Inside Lambda the stream API is not used at all — its in-memory
seen-set does not survive invocations, so the poller calls `subreddit.new()` and
`subreddit.comments()` and dedupes against DynamoDB by fullname.

## Data model

### NormalizedItem

```python
class NormalizedItem(BaseModel):
    item_id: str          # "{source}#{external_id}" — deterministic idempotency key
    source: Literal["github", "reddit"]
    source_kind: str      # issue | issue_comment | discussion | discussion_comment
                          # | submission | comment
    external_id: str
    parent_external_id: str | None   # parent issue for a comment;
                                     # link_id for a reddit comment
    channel: str          # "owner/repo" or "r/subreddit"
    url: str
    author: str           # username only, never email
    title: str | None
    body: str
    created_at: datetime  # from the platform, never ingest time
    ingested_at: datetime
    raw: dict
```

- `item_id` is derived, not random. Webhook retries and overlapping poll windows
  guarantee repeat delivery; a derived key turns that into a harmless
  conditional-write failure.
- `created_at` comes from the platform so evidence orders correctly regardless of
  when the poller noticed it.
- `raw` is retained so the corpus can be reprocessed against improved prompts
  without re-fetching from APIs.
- `author` excludes email. GitHub payloads sometimes carry committer emails;
  the adapter strips them at the boundary so PII never reaches a public issue.

### DynamoDB — one table, no GSIs

| Entity | `pk` | `sk` |
|---|---|---|
| Item | `ITEM#{item_id}` | `META` |
| Cluster | `CLUSTER#{cluster_id}` | `META` |
| Membership | `CLUSTER#{cluster_id}` | `ITEM#{created_at}#{item_id}` |
| Reddit cursor | `CURSOR#reddit` | `{subreddit}` |

There is no "find unprocessed items" access pattern because SQS carries the work
queue. That absence is what eliminates every secondary index.

The membership sort key exploits lexicographic ordering of ISO-8601, so one
`Query` returns a cluster's evidence already ordered for the issue comment.

Cluster records do **not** store an incremented `item_count`. Increments are not
idempotent and SQS redelivery would double-count. The count is derived from the
membership query.

### Ingestion idempotency

```python
table.put_item(Item=item.model_dump(),
               ConditionExpression="attribute_not_exists(pk)")
```

`ConditionalCheckFailedException` means the item was already seen: return
without enqueueing. This sits before any model call, so duplicate deliveries
cost nothing.

### S3 Vectors

Index `items-titan-v2-1024`, cosine, 1024 dimensions. Model and dimension are
encoded in the name because both are immutable per index — a future embedding
model must create a new index rather than silently corrupting this one.

Metadata is intentionally minimal against the 2 KB filterable budget:
`cluster_id`, `classification`, `channel`, `created_at` filterable; `item_id`
non-filterable. No body text — content lives in DynamoDB.

`QueryVectors` returns a **distance**, not a similarity. The conversion lives in
exactly one helper so no comparison operator is ever written twice.

## Pipeline

```
                    ┌─ noise ──────────────────────────────► END
                    │
prefilter ─► classify ─► dedup ─┬─ match ──► comment_sink ──► END
    │                           │
    └─ rejected ─► END          └─ new ────► structure ─► issue_sink ─► END
```

### Prefilter

Its primary purpose is **loop prevention**, not volume reduction. The system
files issues and posts comments, which fire webhooks, which would be ingested
and filed again — an infinite loop with a Bedrock invoice attached.

Two independent guards: reject events whose sender is our own App installation
id, and label every created issue `community-intel` so it is recognisable even
if sender matching fails.

Secondary rejections: other bots, bodies under ~15 characters after stripping,
emoji-only and `+1` content, and structural noise such as "Closed via #123".

Expected rejection rate on GitHub and Reddit is 20–35%.

### Classify

```python
class Classification(BaseModel):
    reason: str          # first, deliberately
    label: Literal["bug", "feature", "noise"]
    confidence: float
```

Field order matters: structured output is generated in order, so `reason` first
means the label is conditioned on the reasoning rather than rationalised after
it.

Cheap model via `MODEL_CLASSIFIER`. Accuracy comes from few-shot examples drawn
from the eval corpus. `confidence` is for monitoring only — self-reported LLM
confidence is poorly calibrated and does not gate routing.

`noise` terminates: item written with its label, no cluster, no vector.

### Dedup

Structural linkage is attempted first and costs nothing. A comment on issue #42
belongs to #42's cluster by construction — GitHub supplies the parent, Reddit
supplies `link_id`. This also handles the items semantic matching is worst at:
bare comments like "same here" carry no standalone meaning and embed to
something that matches everything and nothing.

Semantic matching applies only to items with no tracked parent.

```python
def resolve_cluster(item, classification) -> tuple[str, str]:
    if item.parent_external_id:
        parent = get_item(f"{item.source}#{item.parent_external_id}")
        if parent and parent.cluster_id:
            return parent.cluster_id, "structural"

    hits = query_vectors(
        vector=embed(text_for(item)), top_k=5,
        filter={"classification": classification, "channel": item.channel},
    )
    if hits and similarity(hits[0]) >= MERGE_HIGH:
        return hits[0].metadata["cluster_id"], "semantic"

    return new_cluster_id(), "new"
```

Every item is embedded; clusters have no centroid. Averaging member vectors
produces something that matches neither member well, and cluster identity is
better carried by the nearest member than the mean. It also removes any update
path — vectors are written once.

The known trade-off is chaining, where A matches B and B matches C but C does
not resemble A. At this volume that means an issue accumulating loosely related
evidence, which is visible and correctable. If evals show it, the guard is a
secondary lower-threshold check against the cluster's seed item.

Filtering on `classification` fixes the stage order: classification must precede
dedup.

### Structure

Runs only on new clusters — a minority of items after prefiltering and dedup.
That is the cost architecture: the strong model touches roughly 10–20% of what
arrives.

```python
class Ticket(BaseModel):
    title: str                    # <= 80 chars, imperative
    summary: str
    repro_steps: list[str] | None
    expected: str | None
    actual: str | None
    severity: Literal["low", "medium", "high", "critical"]
    affected_versions: list[str]
```

Optional fields are optional deliberately. The dominant failure mode for
auto-filed tickets is a fluent, plausible, entirely invented reproduction that
costs an engineer an afternoon. The prompt states that absent information stays
absent, and the schema gives the model somewhere honest to put nothing.

### Sinks

New cluster creates an issue and stores `github_issue_number`. An existing
cluster receives a comment carrying source, permalink, author, truncated quote,
and running count — the visible payoff of the dedup work.

If issue creation fails the cluster persists without an issue number, and the
next item landing in it retries creation. The failure repairs itself rather than
requiring reconciliation.

## Consistency

DynamoDB and S3 Vectors cannot commit together. The answer is idempotency rather
than atomicity: every write is keyed on `item_id` and safe to repeat. The
conditional item put no-ops, the membership put overwrites itself, and
`PutVectors` on an existing key is an upsert. Writes are ordered
DynamoDB-then-vector so a crash leaves a recoverable record rather than an
orphan vector.

## Lambda configuration

| Setting | Value | Rationale |
|---|---|---|
| Batch size | 5 | Two model calls per item; limits partial-failure blast radius |
| Timeout | 180s | Headroom for five items at worst case |
| `ReportBatchItemFailures` | enabled | Without it one poison message retries the whole batch, re-spending tokens on items that already succeeded |
| Reserved concurrency | 5 | Caps runaway spend |
| DLQ | after 3 attempts | Makes poison messages visible |

### Failure handling

- Bedrock throttling — adaptive retry, then return to SQS. Backpressure is free.
- Structured-output parse failure — one retry with the validation error
  appended, then DLQ. Deliberately not falling back to `noise`, since a
  mislabelled item disappears silently while a DLQ'd item is visible.
- GitHub API failure — self-heals on the next item in that cluster.

## Evals

Two model stages and two thresholds, all subjective. Without measurement every
future change is a guess.

**Corpus, in 2–3 hours.** Ship ingestion with models off and let it collect ~200
real items for a few days. Label 150 for classification (~90 min). Hand-group
the bugs into clusters (~20 min) — grouping implicitly defines every positive
and negative pair, so no pairwise labelling is ever needed.

**Classifier** — per-class precision and recall, never accuracy. Classes are
imbalanced toward noise, so labelling everything `noise` would score ~75%
accuracy while being useless. The metric that matters is **recall on `bug`**: a
missed bug is the product failing. Bug precision matters less, since a false
positive is closed in seconds.

**Structurer** — an LLM judge on a rubric plus a grounding check that every
claim in `repro_steps` traces to the source. Used for regression detection
("did this change make it worse"), not absolute quality, since the judge is
itself imperfect.

**Dedup** — precision and recall on the merge decision, computed from the
hand-grouped clusters.

### Deriving the thresholds

Compute similarity across all labelled pairs and examine the two distributions.
Choose the cut by asymmetry of harm: a **false merge** buries a real bug as a
comment on an unrelated issue and is invisible; a **false split** creates a
duplicate issue that is visible and trivially closed. Optimise for precision on
the merge decision, target ~0.95, accept the resulting recall.

This yields a band, not a point. Above `MERGE_HIGH` merge; below it, create.
The ambiguous middle resolves to a new cluster in v1 — the conservative
direction — which is why the v1 resolver compares against `MERGE_HIGH` alone.
Record the lower bound `MERGE_LOW` from the same distribution analysis but do
not branch on it yet: it becomes the entry condition for an LLM tiebreak once
evals show the band is populated enough to justify one.

### Flywheel

Every `community-intel` issue a human closes as invalid is a labelled negative;
every DLQ'd item is a hard case. A small script pulling those back into the
corpus turns normal usage into a growing eval set.

## Repository layout

```
community-intel-engine/
├── src/community_intel/
│   ├── adapters/          github.py, reddit.py → NormalizedItem
│   ├── handlers/          webhook.py, reddit_poll.py, pipeline.py
│   ├── pipeline/          graph.py, nodes/, schemas.py, prompts/
│   ├── store.py           boto3: dynamodb + s3vectors
│   ├── github_sink.py
│   └── config.py          pydantic-settings
├── tests/
├── evals/                 corpus.jsonl, run_evals.py
├── infra/                 CDK — the only place AWS resources are declared
└── pyproject.toml         uv
```

Abstractions are included only where they have more than one real
implementation. The adapter interface qualifies with two. Storage and queue
wrappers do not — those are direct boto3 calls. The GitHub sink is a module, not
an interface, because pointing at a different repository is configuration rather
than a second implementation.

## Configuration

Credentials (GitHub App private key, Reddit OAuth) as SSM `SecureString`.
Thresholds and model ids as plain SSM parameters read at cold start, so tuning
`MERGE_HIGH` is a CLI call rather than a deploy.

## IAM

Scoped per function, no wildcards. The webhook Lambda gets `dynamodb:PutItem` on
one table and `sqs:SendMessage` on one queue. The pipeline Lambda adds
`s3vectors:PutVectors`/`QueryVectors` on one index and `bedrock:InvokeModel` on
**specific model ARNs** — which doubles as a spend guard, since an injected
attempt to invoke an expensive model fails at the IAM layer.

## Observability

Structured JSON logs plus EMF metrics: `items_ingested`,
`items_classified{label}`, `clusters_created`, `duplicates_matched`,
`tokens_consumed`.

Three day-one alarms: DLQ depth > 0, pipeline error rate, and an AWS Budgets
alarm on Bedrock spend. The budget alarm is the backstop for the self-loop
scenario — with no human review gate, nothing else would catch it.

## Cost

Everything but Bedrock sits at or near free tier: Lambda, API Gateway, SQS,
DynamoDB on-demand, and S3 Vectors at this scale are rounding errors.

Bedrock is structural: (items surviving prefilter × classifier tokens) +
(new clusters only × structurer tokens). Prefilter and dedup exist to keep both
multipliers small. Compute the figure against current published rates before
committing to a budget.

## Build order

1. CDK skeleton — table, queue, vector index. Deploy empty.
2. GitHub adapter and webhook Lambda. Verify items land. No models yet.
3. Let it collect for several days.
4. Label the corpus, build the eval harness.
5. Classifier plus evals.
6. Dedup, deriving thresholds from the corpus.
7. Structurer and issue sink. First real ticket filed.
8. Reddit adapter last.

Steps 2 and 3 mean the corpus collects itself while other work proceeds. Reddit
is last because it is gated on OAuth approval, which should overlap with real
work rather than block it.

## Risks

| Risk | Mitigation |
|---|---|
| Self-triggering loop via own webhooks | Sender-id filter, `community-intel` label, Budgets alarm |
| Fabricated repro steps in filed tickets | Optional schema fields, explicit grounding instruction, grounding eval |
| Junk issues from auto-filing with no gate | Accepted product decision; label makes them filterable and bulk-closeable |
| Reddit OAuth approval delay | Sequenced last; adapter interface designed up front |
| Chained clusters from single-linkage matching | Visible in issues; seed-item guard available if evals show it |
| Embedding model change invalidating the index | Model and dimension encoded in the index name, forcing a new index |
