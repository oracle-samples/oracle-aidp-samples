// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The information about the Grantee Type.
**/
export enum GranteeType {
    User = "USER",
    Role = "ROLE",
    Group = "GROUP",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace GranteeType {
    export function getJsonObj(obj: GranteeType): GranteeType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: GranteeType): GranteeType {
        return obj;
    }
}

