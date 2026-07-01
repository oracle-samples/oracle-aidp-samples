// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Auth type.
**/
export enum AuthType {
    NoAuth = "NO_AUTH",
    BearerToken = "BEARER_TOKEN",
    Oauth = "OAUTH",
    OciResourcePrincipal = "OCI_RESOURCE_PRINCIPAL"
    
}

export namespace AuthType {
    export function getJsonObj(obj: AuthType): AuthType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AuthType): AuthType {
        return obj;
    }
}

