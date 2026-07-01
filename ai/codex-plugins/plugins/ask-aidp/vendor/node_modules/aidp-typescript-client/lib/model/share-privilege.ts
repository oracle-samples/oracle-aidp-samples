// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a share, which can be an inherited privilege coming from the object higher in the hierarchy.
**/
export enum SharePrivilege {
    Admin = "ADMIN",
    Read = "READ",
    Use = "USE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace SharePrivilege {
    export function getJsonObj(obj: SharePrivilege): SharePrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SharePrivilege): SharePrivilege {
        return obj;
    }
}

