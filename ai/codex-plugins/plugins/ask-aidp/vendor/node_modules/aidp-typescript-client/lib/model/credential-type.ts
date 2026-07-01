// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The type of credential supported by the credential store.
**/
export enum CredentialType {
    SecretToken = "SECRET_TOKEN",
    VaultReference = "VAULT_REFERENCE",
    ServiceAccount = "SERVICE_ACCOUNT",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace CredentialType {
    export function getJsonObj(obj: CredentialType): CredentialType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CredentialType): CredentialType {
        return obj;
    }
}

