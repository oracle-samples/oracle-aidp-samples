# Step 3 - Social Sign-In authentication scheme

Shared Components → **Authentication Schemes → Create → Social Sign-In**:

- **Credential Store** = `SSO_CRED`
- **Authentication Provider** = OpenID Connect
- **Discovery URL** = `https://<IDENTITY_DOMAIN_HOST>/.well-known/openid-configuration`
- **Scope** = `profile,groups` (APEX adds `openid` automatically)
- **Username claim** = `sub`
- **Additional attribute** = `email`
- **Set this scheme as the app's Current scheme.**

Illustrative export shape (placeholders):

```
create_authentication(
  p_scheme_type  => 'NATIVE_SOCIAL',
  p_attribute_01 => <ref to SSO_CRED web credential>,
  p_attribute_02 => 'OPENID_CONNECT',
  p_attribute_03 => 'https://<IDENTITY_DOMAIN_HOST>/.well-known/openid-configuration',
  p_attribute_07 => 'profile,groups',
  p_attribute_09 => 'sub');
```
