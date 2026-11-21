# Security Policy

## Reporting a vulnerability

Please report security issues privately through GitHub's private vulnerability
reporting: open the repository's **Security** tab and choose **Report a
vulnerability** (GitHub Security Advisories). Do not open a public issue for
security problems.

Include the affected version or commit, a description of the issue, and steps
or a proof of concept to reproduce it. You will get an acknowledgement in the
advisory thread; fixes are coordinated there before public disclosure.

## Supported versions

Only the latest release receives security fixes. Please reproduce issues
against the latest release (or `main`) before reporting.

## Handling of credentials

dukascopy-go never stores credentials. They are read at run time:

- Cloud uploads (`s3://`, `r2://`, `gs://`) use the `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and optional `AWS_SESSION_TOKEN` environment
  variables. Uploads without credentials are refused rather than sent
  unsigned.
- Database, Redis and NATS connections take credentials from the URL or the
  corresponding flags.

Recommendations:

- Prefer environment variables (or a secrets manager that injects them) over
  command-line flags. Secrets placed in URLs or flags end up in shell history,
  process listings (`ps`) and CI logs.
- Use least-privilege credentials, e.g. an access key that can only write to
  the target bucket/prefix, and temporary credentials where possible.
- Never commit `.env` files or credentials to the repository.
