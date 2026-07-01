// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a credential.
**/
export enum CredentialPrivilege {
    Use = "USE",
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"
    
}

export namespace CredentialPrivilege {
    export function getJsonObj(obj: CredentialPrivilege): CredentialPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CredentialPrivilege): CredentialPrivilege {
        return obj;
    }
}

