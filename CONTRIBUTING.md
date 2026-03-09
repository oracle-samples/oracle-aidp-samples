# Contributing to this repository

We welcome your contributions! There are multiple ways to contribute.

## Opening issues

For bugs or enhancement requests, please file a GitHub issue unless it's
security related. When filing a bug remember that the better written the bug is,
the more likely it is to be fixed. If you think you've found a security
vulnerability, do not raise a GitHub issue and follow the instructions in our
[security policy](./SECURITY.md).

## Contributing code

We welcome your code contributions. Before submitting code via a pull request,
you will need to have signed the [Oracle Contributor Agreement][OCA] (OCA) and
your commits need to include the following line using the name and e-mail
address you used to sign the OCA:

```text
Signed-off-by: Your Name <you@example.org>
```

This can be automatically added to pull requests by committing with `--sign-off`
or `-s`, e.g.

```text
git commit --signoff
```

Only pull requests from committers that can be verified as having signed the OCA
can be accepted.

## Pull request process

1. Ensure there is an issue created to track and discuss the fix or enhancement
   you intend to submit.
1. Fork this repository.
1. Create a branch in your fork to implement the changes. We recommend using
   the issue number as part of your branch name, e.g. `1234-fixes`.
1. Ensure that any documentation is updated with the changes that are required
   by your change.
1. Ensure that any samples are updated if the base image has been changed.
1. **If your contribution includes a new notebook or example, update the [README.md](./README.md)**
   following the instructions in the [Adding a New Example](#adding-a-new-example) section below.
1. Submit the pull request. *Do not leave the pull request blank*. Explain exactly
   what your changes are meant to do and provide simple steps on how to validate
   your changes. Ensure that you reference the issue you created as well.
1. We will assign the pull request to 2-3 people for review before it is merged.

## Adding a New Example

When contributing a new notebook or example, you **must** update the [README.md](./README.md)
Sample Catalog to include it. Follow these steps:

1. **Place your notebook** in the appropriate directory:
   - `getting-started/` — introductory or foundational examples
   - `data-engineering/ingestion/` — data connectors and loading patterns
   - `data-engineering/transformation/` — pipeline architectures and table formats
   - `ai/ml-datascience/` — ML models, LLM integrations, and AI services
   - `ai/agent-flows/` — agent orchestration examples
   - `shared-utils/` — reusable utilities

2. **Add a row to the correct table** in the README Sample Catalog, under the section that
   matches your notebook's directory. The row must follow this format:

   ```markdown
   | [Display Name](path/to/your_notebook.ipynb) | One-sentence description of what the notebook demonstrates. |
   ```

   - **Display Name**: a short, human-readable title (not the raw filename).
   - **Path**: relative to the repository root, matching the actual file location exactly
     (use `%20` for spaces in filenames).
   - **Description**: a single sentence that clearly states what the example teaches or
     demonstrates. Start with an action verb (e.g., *"Demonstrate..."*, *"Build..."*, *"Show..."*).

3. **Place the row in the right section**. If no existing section fits, discuss adding a new
   one in the issue before opening the pull request.

4. **Verify your entry** renders correctly by previewing the Markdown before submitting.

## Code of conduct

Follow the [Golden Rule](https://en.wikipedia.org/wiki/Golden_Rule). If you'd
like more specific guidelines, see the [Contributor Covenant Code of Conduct][COC].

[OCA]: https://oca.opensource.oracle.com
[COC]: https://www.contributor-covenant.org/version/1/4/code-of-conduct/
