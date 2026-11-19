# Step 2 - APEX Web Credential for the SSO client

Shared Components → **Web Credentials → Create**:

- **Type:** Basic Authentication
- **Static ID:** `SSO_CRED`
- **Client ID** → username field
- **Client Secret** → secret field

The credential stores the secret encrypted. On an app export the secret is **not** carried
(the credential is created with `prompt_on_install => true`, so APEX asks for it on import).
