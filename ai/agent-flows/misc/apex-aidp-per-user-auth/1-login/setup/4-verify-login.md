# Step 4 - Verify the login

1. Get the app's **real Run URL**: click **Run Application** in App Builder while signed in,
   then copy the URL from the address bar (don't right-click / "copy link" on the Run button -
   it may not expose a real link).
2. Paste it into a **fresh incognito window**.
3. Expect: redirect to the identity domain's real login page → after login/consent you land
   back in the app, authenticated.

**Confirm the identity is established:**
- `:APP_USER` = the user's `sub`.
- the `email` attribute is populated.

**Handoff:** this `:APP_USER` is the input to the per-user auth phase
([`../../2-per-user-oac-token/`](../../2-per-user-oac-token/)) - it calls `mint_*_token(:APP_USER)` to
produce an OBO token whose subject is the same user.
