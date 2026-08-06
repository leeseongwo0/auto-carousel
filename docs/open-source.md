# Newsbot open-source scope and governance

## Project identity and distribution

Newsbot is released under the [MIT License](../LICENSE). The Python distribution
name and version are `telegram-news-bot` 0.1.0.

The canonical source distribution is this GitHub repository and its GitHub
releases. Newsbot makes no promise to publish packages to PyPI or another
registry. GitHub issues and pull requests are the public channels for questions,
proposals, and moderation reports.

## Supported public workflow

The reusable v1 workflow is manual and local:

1. Clone the repository and install dependencies locally.
2. Initialize a local SQLite database.
3. Run collection, review, generation, and delivery commands deliberately from
a local shell.
4. Review outputs before any external delivery.

Public configuration and fixtures use synthetic defaults only. They must not
contain live credentials, personal contact information, production channel
identifiers, private document identifiers, or other deployment-specific values.

Production deployments may use private configuration profiles and credentials
outside the repository. Those profiles are not part of the public interface and
must not be committed, copied into examples, or required to evaluate the local
workflow.

## Legacy compatibility

The repository retains compatibility for an existing VPS automation topology:
Telegram collection and approvals, Google Sheets delivery, Asia/Seoul noon
routing, and optional Codex generation. This is legacy compatibility, not the
public default or a hosted Newsbot service. It requires a private production
profile, separately provisioned credentials, and deployment-specific operating
controls.

Legacy behavior remains intentionally available for existing users, but it does
not create a support commitment, a managed-service promise, or a requirement for
new contributors to configure Telegram, Google Sheets, Codex, a VPS, or a
specific time zone.

## Contributions and governance

Contributions are welcome through GitHub pull requests. By contributing, you
certify that every contribution is covered by the Developer Certificate of
Origin (DCO), using the repository's required sign-off process. Contributions
must preserve the public/local default, avoid secrets and private production
data, and include only material that can be distributed under the MIT License.

Newsbot has one maintainer. The maintainer makes final decisions on scope,
releases, moderation, compatibility, and acceptance of contributions. Decisions
are made in GitHub issues and pull requests where practical; the project has no
separate governing board or escalation channel.

The project is maintained on a best-effort basis with no service-level
agreement, response-time guarantee, security-response guarantee, or support
entitlement.

## Repository history and releases

Existing GitHub `main` history is canonical. Do not rewrite published history:
do not force-push, rebase published commits, or replace existing release tags.
Use ordinary commits, pull requests, revert commits, and corrective releases to
address mistakes. This policy preserves reproducibility for users of the GitHub
source distribution.

Historical commit metadata remains part of that preserved provenance and is not
a project support or security contact. New maintainer commits use the GitHub
noreply identity; public contact stays on the repository's GitHub surfaces.
