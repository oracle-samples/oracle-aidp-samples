# Step 1 - Confidential Application in the identity domain

OCI Console → **Identity & Security → Domains → `<your domain>` → Integrated applications →
Add application → Confidential Application**.

| Setting | Value |
|---------|-------|
| **Type** | Confidential Application (holds a client secret). |
| **Allowed grant types** | **Authorization Code** (drives the interactive SSO login). |
| **Redirect URL** | `https://<ADB_HOST>/ords/apex_authentication.callback` - **byte-for-byte**. A trailing-slash or scheme mismatch fails with `invalid_redirect_uri`. |
| **Scopes / claims** | Ensure `profile` and `groups`; add `email` as an attribute. Do **not** hand-add `openid` (APEX adds it; a duplicate throws `invalid_request ... duplicate values`). |

Capture the **Client ID** and **Client Secret** from the app's OAuth configuration tab for
step 2. Keep the secret out of chat/email/git - it goes straight into the APEX Web Credential.
