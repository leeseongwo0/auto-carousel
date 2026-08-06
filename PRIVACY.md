# Privacy

Newsbot is local-first software. The project does not operate a hosted Newsbot service, collect telemetry, or receive a copy of your local database, profiles, observations, drafts, or exports.

## Data processed locally

Depending on the commands you run, Newsbot can process source identifiers, news text, URLs, timestamps, engagement counts, review decisions, generated drafts, and export history. Treat these files as potentially private even when their source material is public. Keep the state directory owner-only, keep profiles and import documents outside the checkout, review exports before sharing them, and delete local data according to your own retention policy.

## Optional external services

The manual/local workflow needs no external service. Telegram, compatible generation providers, Google Sheets, Codex, and VPS automation are optional legacy adapters. Enabling an adapter sends the data required for that operation to the service you configure and subjects it to that service's policies. Newsbot does not enable those adapters or supply credentials by default.

## Repository and support surfaces

Do not post databases, profiles, credentials, session files, private source identities, generated private content, or unsanitized logs in GitHub issues, discussions, or pull requests. Use synthetic reproductions. Report security vulnerabilities through the private process in [SECURITY.md](SECURITY.md).

The maintainer cannot delete data from user-controlled machines or third-party services. Users are responsible for collection authority, consent, retention, access control, and deletion obligations in their jurisdiction.
