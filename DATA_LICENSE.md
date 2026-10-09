# Data licence and attribution

This project reads two public sources at runtime. It commits only a small set of trimmed,
pseudonymised test fixtures; everything else is downloaded when the pipeline runs and is never
committed.

## Sources

| Source | What is read | Terms |
|---|---|---|
| [Apache Software Foundation Jira](https://issues.apache.org/jira/) (REST API v2, anonymous) | Issues of the KAFKA, SPARK and FLINK projects: key, type, status, priority, resolution, timestamps, fix/affects versions, components, labels, status changelog; and each project's versions (releases) | Contributions to ASF projects, including issue-tracker content, are licensed to the ASF and the public under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) (section 1 defines a "Contribution" to include communication on issue tracking systems managed by the Licensor). The [ASF privacy policy](https://privacy.apache.org/policies/privacy-policy-public.html) applies to personal data. |
| [GitHub REST API](https://docs.github.com/en/rest) for [apache/kafka](https://github.com/apache/kafka), [apache/spark](https://github.com/apache/spark), [apache/flink](https://github.com/apache/flink) | Pull requests (title, state, timestamps, head SHA, labels), reviews (state, time, reviewed commit), check runs on the final head commit, default-branch commits (subject line and PR reference), releases | Use of the API is governed by the [GitHub Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service) and [Acceptable Use Policies](https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies) (including the information-usage restrictions on personal data). PR titles and commit subjects in these repositories are contributions to ASF projects under the Apache License 2.0. |

Independent sources used only for verification (fetched at runtime, nothing stored): the
[Kafka downloads page](https://kafka.apache.org/community/downloads/), the
[Spark news page](https://spark.apache.org/news/index.html), the
[Flink downloads page](https://flink.apache.org/downloads/) and the Kafka `RELEASE_NOTES.html`
files on the [ASF archive](https://archive.apache.org/dist/kafka/).

## What is committed

| Path | Content | Licence |
|---|---|---|
| Source code, SQL, Terraform, docs | Written for this project | MIT ([LICENSE](LICENSE)) |
| `tests/fixtures/api/jira/*.json` | 154 real Jira issues (48 KAFKA, 58 SPARK, 48 FLINK) and every version of the three projects, recorded on 9 October 2026 and trimmed to the fields the pipeline reads | Apache License 2.0 (ASF contributions); see attribution below |
| `tests/fixtures/api/github/*/*.json` | 111 real pull requests with their reviews and check runs, and 42 default-branch commits, recorded on 9 October 2026 and trimmed | Apache License 2.0 (ASF contributions); see attribution below |

Before committing, `scripts/build_fixtures.py`:

* replaces every Jira username and GitHub login with a stable pseudonym (`user-<8 hex>`);
* drops display names, e-mail addresses, avatars, URLs, PR bodies, comments and commit message
  bodies (only the subject line and the `Closes #123` style reference are kept, with the
  contributor's login in that line pseudonymised);
* keeps the remaining values (keys, titles, states, timestamps, versions) unmodified.

The pipeline itself never stores people's names either: the curated layer replaces every
identity with an HMAC pseudonym (`u_<12 hex>`, see `src/dx_productivity/pseudonym.py`), and the
results site shows only aggregates. This is pseudonymisation, not anonymisation: the source data
is public and the default salt is public.

## Simulated data

None of the published metrics are simulated. The only synthetic change anywhere is in the test
suite: `tests/e2e/test_pipeline.py::test_new_activity_is_picked_up_incrementally` edits a
temporary copy of one fixture issue (marks it Resolved with a later timestamp) to prove that an
incremental run picks up new activity. That copy lives in a pytest temp directory, is labelled
as simulated in the test, and is never committed or published.

## Attribution

If you publish anything built with this pipeline:

> Contains issue data from the Apache Software Foundation Jira (https://issues.apache.org/jira/)
> and pull-request data from the apache/kafka, apache/spark and apache/flink GitHub
> repositories, licensed under the Apache License 2.0.

"Apache", "Apache Kafka", "Apache Spark" and "Apache Flink" are trademarks of the Apache
Software Foundation. "Atlassian" and "Jira" are trademarks of Atlassian; "GitHub" is a trademark
of GitHub, Inc. This project is not affiliated with or endorsed by any of them.

This is the author's reading of the terms above, not legal advice. If you are a contributor and
want a record removed from the fixtures, open an issue.
