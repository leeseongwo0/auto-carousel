# Contributing to Newsbot

Newsbot has a single maintainer and accepts contributions on a best-effort basis. There is no service-level agreement and no promise of review or response time.

## Before opening a pull request

- Keep changes focused and preserve the manual/local workflow as the public default.
- Do not add secrets, tokens, credentials, private configuration, production data, or personally identifiable information (PII) to code, tests, documentation, commits, issues, or pull requests.
- Use synthetic, public, or appropriately authorized data in examples and tests.
- Add or update focused tests for behavior changes.
- Run the repository checks applicable to your change:

  ```bash
  uv run pytest
  uv run ruff check .
  uv run mypy
  ```

- Explain the user-visible change and any compatibility impact in the pull request.

## Developer Certificate of Origin

All commits must include a Developer Certificate of Origin (DCO) sign-off. Use:

```bash
git commit -s -m "Describe the change"
```

By signing off, you certify that you have the right to submit the contribution under the repository's license, consistent with the Developer Certificate of Origin.

## Conduct and review

Participate respectfully under the [Contributor Covenant](CODE_OF_CONDUCT.md). Maintainer decisions on scope, compatibility, and acceptance are final.
