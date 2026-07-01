// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a recipient
**/
export enum RecipientPrivilege {
    Admin = "ADMIN",
    Use = "USE",
    Read = "READ",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace RecipientPrivilege {
    export function getJsonObj(obj: RecipientPrivilege): RecipientPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: RecipientPrivilege): RecipientPrivilege {
        return obj;
    }
}

