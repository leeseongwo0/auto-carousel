# Release policy

The canonical distribution surface is the GitHub repository. Version 0.1.0 is not published to PyPI.

A release candidate must be built from the exact reviewed commit in a clean checkout. Discard pre-existing local `dist/` files; never attach operator bundles, databases, profiles, credentials, sessions, reports, or runtime output. Verify the wheel and source archive contain the MIT license, package metadata, intended migrations and schemas, and no private material.

Published `main` history is append-only. Use pull requests, ordinary commits, reverts, and corrective releases. Do not force-push, replace published tags, or treat deletion or visibility changes as retraction.

Before a GitHub release, require the repository CI check, a clean dependency and secret audit, protected `main`, and a candidate-bound hosted-surface review. GitHub release assets are optional; PyPI publication and production deployment are outside this policy.
