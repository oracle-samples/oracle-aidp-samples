# Step 2 - OCI Vault and KMS signing key

This key signs the JWT assertions used to mint each user's OAC token - the private key
never leaves OCI Vault/HSM, so signing always happens through a remote KMS Sign call
rather than local key material.

1. OCI Console -> **Identity & Security -> Vault -> Create Vault** (a default,
   non-virtual vault is fine).
2. Inside the vault -> **Keys -> Create Key**:
   - Protection mode: **HSM**
   - Key shape: **Asymmetric**, algorithm **RSA**, length **2048**
   - Key usage: **Sign and Verify**
3. Record the key's full **OCID** and the current **Key Version OCID** (**Versions**
   tab) - both are needed in step 3 and step 7.
