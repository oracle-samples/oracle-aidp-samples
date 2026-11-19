# Step 3 - Self-signed certificate for the Vault key

The identity domain needs an X.509 certificate to verify JWTs signed by this key.
Because the private key never leaves the Vault/HSM, you can't self-sign it the normal
`openssl req -x509 -key ...` way - instead, build the certificate locally but obtain the
actual RSA signature through a remote call to OCI KMS Sign.

## Prerequisites

- Authenticate a local OCI CLI session (used only for the one-time steps in this file,
  not for the database's own runtime credential from step 1):
  ```bash
  oci session authenticate --region <oci_region>
  ```
  Save the profile name it prompts you for as `<your_authenticated_profile>` - the OCI
  CLI commands below and the script in step 3.3 both reference it.
- Add `--config-file ~/.oci/config --auth security_token --profile <your_authenticated_profile>`
  to OCI CLI commands so they authenticate with this session instead of a static API key.

## 3.1 Get the public key

```bash
oci kms management key-version get \
  --key-id <key_OCID> \
  --key-version-id <key_version_OCID> \
  --endpoint <vault_management_endpoint> \
  --config-file ~/.oci/config --auth security_token --profile <your_authenticated_profile>
```

(The vault's Management endpoint is on the vault's overview page.) Copy the PEM value
from the response (starts `-----BEGIN PUBLIC KEY-----`).

## 3.2 Grant Sign permission to the system user

Find the system user's group (Identity & Security -> Domains -> Users -> `<system user
from step 1>` -> Groups), then as a tenancy admin:

```
Allow group <System User's Group> to use keys in compartment <vault_compartment>
where target.key.id = '<key_OCID>'
```

## 3.3 Generate the certificate

On your local workstation, create a virtual environment and install the required
packages:

```bash
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install oci cryptography
```

> **Gotcha:** Homebrew Python blocks a system-wide `pip install` with an
> `externally-managed-environment` error - use a venv, as shown above.

Save the following as `pub_key_to_cert.py`, filling in the constants at the top:

```python
# pub_key_to_cert.py
import base64
import datetime
import hashlib
import oci
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

KEY_ID = "<key OCID>"
KEY_VERSION_ID = "<key version OCID>"
CRYPTO_ENDPOINT = "<vault CRYPTO endpoint, not management>"
PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
...paste from step 3.1...
-----END PUBLIC KEY-----
"""

# Use a profile authenticated through `oci session authenticate` (security-token auth) -
# this is a *local, one-time* step to generate the certificate; it is not the system
# user's own API key from step 1, which authenticates the database's runtime calls.
config = oci.config.from_file(profile_name="<your-authenticated-profile>")
with open(config["security_token_file"]) as f:
    token = f.read()
private_key = oci.signer.load_private_key_from_file(config["key_file"])
signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
crypto_client = oci.key_management.KmsCryptoClient(config, signer=signer, service_endpoint=CRYPTO_ENDPOINT)
public_key = serialization.load_pem_public_key(PUBLIC_KEY_PEM)

class KmsRSAPrivateKey(rsa.RSAPrivateKey):
    def public_key(self):
        return public_key

    @property
    def key_size(self):
        return public_key.key_size

    def sign(self, data, padding_alg, algorithm):
        # OCI KMS Sign caps a RAW message at ~245 bytes for a 2048-bit key, so hash
        # locally and send the digest instead (KMS applies the correct padding).
        digest = hashlib.sha256(data).digest()
        details = oci.key_management.models.SignDataDetails(
            key_id=KEY_ID,
            key_version_id=KEY_VERSION_ID,
            message=base64.b64encode(digest).decode(),
            message_type="DIGEST",
            signing_algorithm="SHA_256_RSA_PKCS1_V1_5",
        )
        response = crypto_client.sign(details)
        return base64.b64decode(response.data.signature)

    def decrypt(self, ciphertext, padding_alg):
        raise NotImplementedError

    def private_numbers(self):
        raise NotImplementedError

    def private_bytes(self, encoding, format, encryption_algorithm):
        raise NotImplementedError

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "oac_chat_jwt_signing_cert")])

builder = (
    x509.CertificateBuilder()
    .subject_name(name)
    .issuer_name(name)  # self-signed
    .public_key(public_key)
    .serial_number(x509.random_serial_number())
    .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
    .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650))
)

cert = builder.sign(private_key=KmsRSAPrivateKey(), algorithm=hashes.SHA256())
print(cert.public_bytes(serialization.Encoding.PEM).decode())
```

```bash
python3 pub_key_to_cert.py > oac_chat_jwt_signing_cert.pem
openssl x509 -in oac_chat_jwt_signing_cert.pem -text -noout   # should parse cleanly
```

**Gotchas hit while building this:**
- `RSAPrivateKey` subclasses in newer `cryptography` versions also require `__copy__`
  and `__deepcopy__`, or you get `TypeError: Can't instantiate abstract class`.
- `datetime.utcnow()` is deprecated - use `datetime.now(datetime.timezone.utc)`.
- KMS Sign's `message_type="RAW"` has a small max size (245 bytes for RSA-2048) -
  always hash locally and use `message_type="DIGEST"` for anything non-trivial.
- The identity domain's certificate import expects a real X.509 certificate
  (`BEGIN CERTIFICATE`), not a bare public key (`BEGIN PUBLIC KEY`) - hence this script.
- The certificate is valid for 10 years (`not_valid_after`). The underlying Vault key
  has no forced expiry - only the certificate does - so plan a renewal well before that
  date; nothing warns you as it approaches. A confidential app can only import **one**
  certificate, so there's no built-in overlap window for a graceful swap - see the
  gotcha in step 4 about the certificate's Alias.
