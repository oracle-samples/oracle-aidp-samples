# Step 1 - System user and API key

The database needs its own OCI identity to call KMS Sign (step 3) and, depending on how
your agent is deployed, to OCI-sign the agent call itself. Use a dedicated **system
user**, not a developer's personal account - it's the credential a Web Credential will
hold long-term, and it should be independently auditable and revocable.

1. OCI Console -> **Identity & Security -> Domains -> `<your domain>` -> User
   management -> Users -> Create**.
2. Specify the system user's details, with "Use the email address as username" enabled.
3. Add it to whatever group your IAM policies below will target.
4. **Create.**
5. Open the new user -> **API Keys** tab -> **Add API key** -> **Generate API key
   pair**.
6. Download the private and public keys. **Add.**
7. Record the **tenancy OCID**, **user OCID**, and **fingerprint** shown after adding
   the key - step 5 (Web Credential) needs all three.

> **Note:** this same user needs permission to trigger your AIDP agent, in addition to
> the Vault Sign policy added in step 3 - check with whoever owns the agent's IAM
> policy, since it's typically a separate policy from the Vault Sign grant (one doesn't
> imply the other).
