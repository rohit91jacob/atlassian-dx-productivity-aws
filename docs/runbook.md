# Runbook

Exit codes of `dx`: `0` ok, `1` failure, `2` usage or configuration error, `3` data-quality
failure (raw, curated or mart gate, including failed dbt tests), `4` verification mismatch
(`dx verify --strict` only).

## Daily operation

| Where | What runs | Schedule |
|---|---|---|
| GitHub Actions (`refresh.yml`) | `dx extract` -> `dx run --skip-extract` -> `dx verify` -> `dx report` -> Pages | 05:30 UTC daily |
| AWS (if deployed, `infra/`) | EventBridge Scheduler -> extract Lambda (`{"sources":["jira"]}` and `{"sources":["github"]}`), then the curate Lambda; dbt on Athena from `deploy.yml` | 04:15 / 04:45 UTC daily |

Freshness: the site header shows the evaluation time; `summary.json` has `as_of`; the coverage
table shows the latest `updated` per source. In AWS, the `<function>-no-runs` alarm fires when a
function has not run for 26 hours.

## First run and backfill

The first run has no watermark, so it starts `DX_JIRA_LOOKBACK_DAYS` / `DX_GITHUB_LOOKBACK_DAYS`
back. Jira: 60 days of KAFKA + SPARK + FLINK is about 4,200 issues, 60 pages, 3 minutes at the
default 1 request/second. GitHub: PR enrichment costs one request per PR (plus one for closed
PRs' check runs), so a large backlog drains over several runs within `DX_GITHUB_MAX_REQUESTS`.
Nothing is skipped: the `pulls` watermark only passes a PR once its details are landed.

To backfill further than the first window, reset the watermark and raise the lookback:

```bash
uv run dx state --prefix "watermark#jira#KAFKA#issues" --reset --yes
DX_JIRA_LOOKBACK_DAYS=365 uv run dx extract --sources jira
uv run dx curate
```

## Rebuilding curated tables

`uv run dx curate --full-refresh` re-merges every raw object. The result is identical to the
incremental one (tested). Do it after adding a curated table or column, or after changing
`normalize.py`, because already-processed raw objects are not revisited otherwise.

## Data-quality failure (exit 3)

1. Find the layer: `ops/dq_results/dt=<today>/<run_id>-{raw,curated,marts}.json` (or the dbt
   output for dbt tests). Each failed check lists up to 5 sample violations.
2. **raw**: an envelope with a non-200 page or an unexpected body shape. Nothing was curated.
   Inspect the object named in the sample. If it is an API error page that slipped through,
   delete that one object (raw is otherwise immutable) and run `dx curate` again; the extractor
   will refetch on its next run because the watermark only advances past landed pages.
3. **curated**: duplicate keys, orphans or impossible timestamps in the merged tables. Nothing
   was published (curated tables keep their previous snapshot). Usually a normalizer bug after
   an API change: fix, add a unit test with the offending record, `dx curate --full-refresh`.
4. **marts** / dbt test: check the failing model's SQL against the data
   (`uv run dx sql "..."`). Warnings (`warn` severity) never block.

## GitHub rate limits

* Unauthenticated: 60 requests/hour per IP (shared with anything else on that IP). The run stops
  with `stopped: "rate_limited"` or `"budget"` and keeps its watermarks; run again after the
  reset. Set `GITHUB_TOKEN` (read-only, public repositories) for 5,000/hour.
* Actions `GITHUB_TOKEN`: 1,000 requests/hour per repository; `refresh.yml` uses a 900 budget.
* Secondary rate limits (HTTP 403/429 with `Retry-After`) are retried automatically.

## Jira availability

The ASF Jira is a shared, volunteer-run service. Requests are paced at one per second and
retried with backoff on 429/5xx. If it is down, the Jira stream fails and the GitHub stream is
unaffected (separate schedules in AWS; `dx extract --sources github` locally).

## Scheduled refresh fails

The `alert` job opens (or comments on) an issue titled "Scheduled refresh is failing".

1. Open the failed run. Extraction state is saved to the Actions cache right after `dx extract`,
   even when a later step fails, so progress is never lost.
2. Exit 3 is a data-quality gate: follow the section above.
3. A cache miss (evicted after 7 days unused) makes the next run a first run (lookback window);
   results are complete again after the GitHub backlog drains.
4. Re-run with **Run workflow** (`full_refresh` rebuilds the curated tables). Close the issue once
   a run is green.

## Concurrency

Watermark updates are conditional writes on `version`; a second concurrent extractor loses with
`ConflictError` instead of corrupting state. In AWS each function also has reserved concurrency
1, and `refresh.yml` has a workflow-level concurrency group.

## Credentials

* Local: optional `GITHUB_TOKEN` env var; never commit it (`.env` is git-ignored, gitleaks runs
  in pre-commit and CI).
* Actions: the per-run `GITHUB_TOKEN` only.
* AWS: the extractor reads the token from Secrets Manager (`<name>/github-token`). Rotate it in
  GitHub, then `aws secretsmanager put-secret-value`. No AWS keys exist anywhere: `deploy.yml`
  assumes the deploy role through GitHub OIDC.

## Deploying to AWS (operator)

Not done for this portfolio (no cloud account is used); the steps are:

1. `make lambda-zip`; build and push a first curate image tagged `latest-release`
   (`docker build -f infra/lambda/curate.Dockerfile ...`).
2. `cd infra && terraform init -backend-config=backend.hcl && terraform apply`
   (`backend.hcl`: S3 bucket, key, region, `use_lockfile = true`).
3. Put the GitHub token in the secret from output `github_token_secret_arn`.
4. Set repository variables `AWS_DEPLOY_ROLE_ARN`, `DX_EXTRACT_FUNCTION`, `DX_CURATE_FUNCTION`,
   `DX_CURATE_REPOSITORY_URL`, `DX_ATHENA_WORKGROUP`, `DX_ATHENA_STAGING_DIR`,
   `DX_ATHENA_DATA_DIR`, `DX_ATHENA_CURATED_DATABASE` from the outputs; add reviewers to the
   `aws` environment. `deploy.yml` then ships code and runs dbt on Athena.
5. Costs are capped by the budget alarm (`monthly_budget_usd`), Athena's per-query scan limit,
   reserved concurrency 1 and S3 lifecycle rules.
