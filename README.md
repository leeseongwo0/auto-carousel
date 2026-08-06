# Newsbot

Newsbot is a local-first workflow for turning news observations into reviewed, exportable drafts. The public v1 workflow is manual and local: you define the sources, import synthetic or user-collected observations, choose candidates, review drafts, and write approved exports to directories you control.

The Python distribution remains `telegram-news-bot` version `0.1.0`; its installed command is `newsbot`.

## Install

Clone this repository, then install it into an environment you control. There is no package-registry release.

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install .
newsbot --help
```

For development tools, use `uv sync --group dev` from the checkout.

## Manual/local v1

### Keep state private

Create a private state directory before initialization. Newsbot creates its SQLite database there and keeps the database owner-readable and owner-writable (`0600`). Keep the state directory at `0700`; do not place it in a shared directory or commit it.
For the supported trust boundary, every existing ancestor of the state and output paths must be owned by either the current user or `root`, must not be a symbolic link, and must not be writable by group or other users. Shared or sticky locations such as `/tmp` are unsupported, even when the final directory itself is private.

```bash
export NEWSBOT_STATE="$HOME/.local/state/newsbot"
export NEWSBOT_PROFILE="$NEWSBOT_STATE/profile.toml"
export NEWSBOT_INPUT="$NEWSBOT_STATE/observations.json"
export NEWSBOT_OUTPUT="$HOME/.local/share/newsbot/output"
install -d -m 700 "$NEWSBOT_STATE"
install -d -m 700 "$NEWSBOT_OUTPUT/candidates" "$NEWSBOT_OUTPUT/drafts" "$NEWSBOT_OUTPUT/exports"
```

Use an explicit output directory for every command that materializes data. Outputs are separate from private state, so they can be inspected, backed up, or selectively shared without sharing the database.
Newsbot supports Python 3.12 or newer on POSIX/Linux systems.

### Define sources and import observations

Create a local behavior profile at `$NEWSBOT_PROFILE`. It must use `schema = "newsbot.behavior.v1"` and `operation = "manual_local"`, contain 1–32 enabled `[[sources]]`, and include the required `[policy]` and `[news_policy]` tables. Each source has a user-defined ID, name, classification, priority, quality, and domain lists; a public Telegram handle is optional. Use synthetic sources for demonstrations and tests, or sources you are authorized to collect.
Start from the tracked synthetic template and keep your real profile in the private state directory:

```bash
cp config/manual-profile.example.toml "$NEWSBOT_PROFILE"
```

Import data is a bounded JSON document stored at `$NEWSBOT_INPUT`, outside the checkout and with mode `0600`. Set `schema` to `newsbot.manual.import.v1` and provide a `records` array. Every record identifies a source from the profile and includes a post ID and timezone-aware `published_at`; text, URLs, and engagement values are optional. The document may contain at most 10,000 records and be at most 16 MiB.

```bash
newsbot manual-init --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3
newsbot manual-import --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --input "$NEWSBOT_INPUT"
```
As an optional alternative to a local import, install the `telegram` extra, add explicit public `telegram_handle` values to the private profile, and provide `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and an owner-only `TELEGRAM_SESSION_PATH` outside the checkout. The following one-shot command processes sources sequentially, performs at most the stated pages per source, stops at the deadline, and leaves a durable cursor for a later manual continuation. It does not rank, generate, approve, or export anything.

```bash
newsbot manual-collect-telethon --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --lookback-hours 24 --page-limit 100 --max-pages 10 --deadline-seconds 900
```

### Rank, select, generate, review, and export

The commands print IDs in their JSON results. Substitute those returned IDs in the later commands; the angle-bracket values below are placeholders, not literal arguments.

```bash
# Read RUN_ID from the manual-rank result.
newsbot manual-rank --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3
CANDIDATES_JSON="$(newsbot manual-candidates --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --run-id <RUN_ID> --output-dir "$NEWSBOT_OUTPUT/candidates")"
CANDIDATE_ARTIFACT="$(python -c 'import json,sys; from pathlib import Path; result = json.loads(sys.argv[1]); receipt = result["receipt"]; filename = result["artifact_filename"]; expected = f"candidates-{sys.argv[3]}-{receipt}.json"; assert len(receipt) == 64 and all(char in "0123456789abcdef" for char in receipt) and filename == expected, "command artifact identity is invalid"; path = Path(sys.argv[2]) / filename; preview = json.loads(path.read_text(encoding="utf-8")); assert preview["receipt"] == receipt, "artifact receipt does not match command receipt"; print(path)' "$CANDIDATES_JSON" "$NEWSBOT_OUTPUT/candidates" "<RUN_ID>")"
CANDIDATE_RECEIPT="$(python -c 'import json,sys; print(json.loads(sys.argv[1])["receipt"])' "$CANDIDATES_JSON")"
python -m json.tool "$CANDIDATE_ARTIFACT"
newsbot manual-candidate-decision --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --run-id <RUN_ID> --candidate-id <CANDIDATE_ID> --decision select --expected-receipt "$CANDIDATE_RECEIPT"
# Each decision changes the candidate set. Before every later decision, rerun manual-candidates and repeat the command-result receipt and artifact validation above; use only that new receipt with the next decision.
newsbot manual-generate --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --candidate-id <CANDIDATE_ID> --provider fake --page-count 1 --output-dir "$NEWSBOT_OUTPUT/drafts"

# Read GENERATION_ID from manual-generate and bind review to the exact draft bytes.
DRAFT_DIGEST="$(sha256sum "$NEWSBOT_OUTPUT/drafts/draft-<GENERATION_ID>.json" | cut -d' ' -f1)"
newsbot manual-review --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --candidate-id <CANDIDATE_ID> --generation-id <GENERATION_ID> --decision approve-local --expected-draft-digest "$DRAFT_DIGEST"
newsbot manual-export --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3 --output-dir "$NEWSBOT_OUTPUT/exports"
newsbot manual-status --profile "$NEWSBOT_PROFILE" --state "$NEWSBOT_STATE" --database newsbot.sqlite3
```

`--provider fake` is suitable for local demonstrations. `--provider openai_compatible` requires its documented environment configuration and sends generation requests to the configured compatible provider; do not put credentials in the profile, input, output, or repository.
`manual-generate` already writes the current draft to its `--output-dir`. `manual-draft` is an optional rematerialization command; when needed, point it at a different empty directory rather than the generation destination.

## Legacy compatibility

Telegram collection and approvals, Google Sheets delivery, Asia/Seoul scheduled routing, systemd workers, and Codex generation remain compatibility adapters for existing deployments. They are not the public default and are not required for the manual/local workflow. Their commands and service definitions remain available for migration and maintenance, but do not enable an adapter unless its dependencies, credentials, and operational boundaries are intentionally configured.

Useful compatibility entry points: [`deploy/systemd/`](deploy/systemd/), [`newsbot --help`](#install), and the repository's [Issues](../../issues).

## Project information

Newsbot is MIT licensed. Contributions require DCO sign-off; see [CONTRIBUTING.md](CONTRIBUTING.md). Read [PRIVACY.md](PRIVACY.md) before importing real observations. Release policy and user-visible changes are documented in [RELEASES.md](RELEASES.md) and [CHANGELOG.md](CHANGELOG.md). Security reports belong in [GitHub Private Vulnerability Reporting or Security Advisories](../../security/advisories/new), not public issues; see [SECURITY.md](SECURITY.md). For public usage questions, use [GitHub Issues or Discussions](../../discussions); see [SUPPORT.md](SUPPORT.md).
