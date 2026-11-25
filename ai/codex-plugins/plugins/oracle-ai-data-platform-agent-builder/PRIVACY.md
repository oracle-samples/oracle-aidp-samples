# Privacy

Effective November 24, 2026.

This sample runs in the user's coding environment and against their selected
Oracle AI Data Platform resources. It sends no telemetry or usage reports to
the sample maintainers.

## Data flow

- Codex processes the requirements, code and context supplied to the builder
  under the user's Codex configuration and service terms.
- The builder writes source, English contracts, package files and deployment
  plans into user-selected directories. These can contain business information.
- Publishing uses the user's existing AIDP operations tools or CLI. Those tools
  authenticate to AIDP and upload the selected code and resources.
- The bundled loopback gateway calls the configured native A2A agent through
  the user's OCI CLI profile. Browser requests, questions and returned business
  data pass through that local gateway. OCI credentials remain host-side.
- Generated reasoning agents and their data tools can send authorized business
  context to configured OCI Generative AI models or other declared tools.

## Local state and credentials

The shared client stores private transport context IDs, command receipts and
responses under the configured state directory. This is not a replacement for
the native agent's business-process checkpointer. The gateway also creates a
private per-run access token; it is not an OCI credential. The browser receives
an HttpOnly, SameSite cookie, and CLI access uses the private local token.

The OCI CLI reads the user's existing OCI configuration. The plugin does not
copy OCI keys, security tokens or passwords into generated agent code or the
browser. Do not distribute completed target configurations, private state,
credentials, reports or prepared business datasets with this plugin.

The maintainer does not receive local artifacts automatically. Publishing them
to GitHub or another service is a separate user action. GitHub marketplace
installation and Codex usage remain subject to those services' privacy terms.

For sample questions, use the repository's GitHub issues.
